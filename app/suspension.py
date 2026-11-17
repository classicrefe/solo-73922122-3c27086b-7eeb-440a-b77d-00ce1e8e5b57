"""Data-supply suspension / resume service.

A suspension targets a *released* batch only and never touches the original
decision, the rolling reference window or background processing.  Two tables
back the feature (see schema.py):

* ``batch_suspensions`` -- one row per currently-suspended batch (the
  authoritative state; primary key on batch id);
* ``batch_suspension_events`` -- append-only SUSPEND/RESUME audit trail.

Withholding is derived, not stored.  The suspended set is expanded on every
read with a recursive CTE that walks ``batches.reference_ids``: a released
batch is withheld when it *is* suspended or directly/transitively references a
suspended batch as a decision reference.  Because the walk is computed at read
time, batches decided after a suspension (including on the other API
instance) are withheld automatically, and resuming one batch restores exactly
the batches no longer reachable from any remaining suspension.

Concurrency: every state change runs in one transaction while holding
``SELECT ... FOR UPDATE`` on the batch row, so two instances issuing
interleaved suspend/resume calls serialise and can neither lose a suspension
nor leave the suspension table inconsistent with data-lake eligibility.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from .config import settings


class SuspensionError(Exception):
    """Business-rule violation on a suspend/resume request (mapped to 4xx)."""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


# Predicate "batch b references the withheld id w.id".  Using the jsonb
# containment operator lets the GIN index on reference_ids back the recursive
# walk; COALESCE keeps NULL (undecided) reference windows safe.
_REFS = "COALESCE(b.reference_ids, '[]'::jsonb) @> to_jsonb(w.id)"

# Recursive walk over the reference graph.  Seed rows are the currently
# suspended batches; each recursive step adds every released batch whose
# reference window contains an already-withheld batch id.  UNION (not UNION
# ALL) makes the walk cycle-safe; the graph itself is a DAG in practice.
_WITHHELD_CTE = f"""
WITH RECURSIVE withheld(id) AS (
    SELECT batch_id FROM batch_suspensions
    UNION
    SELECT b.id
      FROM batches b
      JOIN withheld w ON {_REFS}
     WHERE b.status = 'RELEASED'
)
"""


async def _batch_nos_conn(conn: asyncpg.Connection,
                          batch_ids: List[int]) -> List[str]:
    if not batch_ids:
        return []
    rows = await conn.fetch(
        "SELECT batch_no FROM batches WHERE id = ANY($1::bigint[]) "
        "ORDER BY batch_no",
        batch_ids,
    )
    return [r["batch_no"] for r in rows]


# Recursive sub-tree rooted at one batch id (the batch plus every released
# batch that directly/transitively references it).
_SUBTREE_SQL = (
    "WITH RECURSIVE sub(id) AS ("
    "SELECT $1::bigint "
    "UNION "
    "SELECT b.id FROM batches b JOIN sub s "
    "ON COALESCE(b.reference_ids, '[]'::jsonb) @> to_jsonb(s.id) "
    "WHERE b.status = 'RELEASED'"
    ") SELECT id FROM sub"
)


async def _withheld_ids_conn(conn: asyncpg.Connection,
                             root_id: Optional[int] = None) -> List[int]:
    if root_id is None:
        rows = await conn.fetch(_WITHHELD_CTE + "SELECT id FROM withheld")
    else:
        rows = await conn.fetch(_SUBTREE_SQL, root_id)
    return [r["id"] for r in rows]


async def _fetch_batch(conn, batch_no: str) -> Optional[asyncpg.Record]:
    return await conn.fetchrow(
        "SELECT id, batch_no, device_id, status FROM batches "
        "WHERE batch_no = $1",
        batch_no,
    )


async def suspend_batch(pool: asyncpg.pool.Pool, *, batch_no: str,
                        reason: str, actor: Optional[str] = None) -> Dict[str, Any]:
    """Suspend data supply for one released batch; repeat calls are idempotent.

    Uses exactly one pooled connection for the whole call (the post-commit
    read happens on that same connection), so concurrent calls can never block
    each other waiting for a second connection while holding one -- the pool
    cannot self-deadlock regardless of its size.
    """
    reason = (reason or "").strip()
    if not reason:
        raise SuspensionError(422, "暂停原因不能为空 / reason is required")
    actor = (actor or settings.instance_id).strip() or settings.instance_id

    async with pool.acquire() as conn:
        async with conn.transaction():
            batch = await _fetch_batch(conn, batch_no)
            if batch is None:
                raise SuspensionError(404,
                                      f"批号不存在 / batch not found: {batch_no}")
            if batch["status"] != "RELEASED":
                raise SuspensionError(
                    409,
                    f"仅已放行批可暂停，当前状态为 {batch['status']} / only "
                    "RELEASED batches can be suspended")
            # Row lock serialises concurrent suspend/resume on this batch.
            await conn.execute("SELECT id FROM batches WHERE id = $1 FOR UPDATE",
                               batch["id"])
            existing = await conn.fetchrow(
                "SELECT reason, created_at, created_by FROM batch_suspensions "
                "WHERE batch_id = $1",
                batch["id"],
            )
            if existing is not None:
                # Idempotent repeat: keep the authoritative reason/state, but
                # leave an audit entry showing the duplicate request.
                await conn.execute(
                    "INSERT INTO batch_suspension_events"
                    "(batch_id, action, reason, actor) VALUES ($1,'SUSPEND',$2,$3)",
                    batch["id"], reason, actor)
                changed = False
                susp = existing
            else:
                await conn.execute(
                    "INSERT INTO batch_suspensions(batch_id, reason, created_by) "
                    "VALUES ($1, $2, $3)",
                    batch["id"], reason, actor)
                await conn.execute(
                    "INSERT INTO batch_suspension_events"
                    "(batch_id, action, reason, actor) VALUES ($1,'SUSPEND',$2,$3)",
                    batch["id"], reason, actor)
                changed = True
                susp = await conn.fetchrow(
                    "SELECT reason, created_at, created_by FROM batch_suspensions "
                    "WHERE batch_id = $1", batch["id"])
        # Transaction committed; compute the response on the same connection.
        withheld_ids = await _withheld_ids_conn(conn, root_id=batch["id"])
        withheld_nos = await _batch_nos_conn(conn, withheld_ids)

    return {
        "batch_no": batch_no,
        "device_id": batch["device_id"],
        "suspended": True,
        "changed": changed,
        "reason": susp["reason"],
        "suspended_at": susp["created_at"].isoformat(),
        "suspended_by": susp["created_by"],
        "withheld_batch_nos": withheld_nos,
        "message": ("该批已处于暂停状态，重复请求未改变状态 / already suspended, "
                    "no state change"
                    if not changed
                    else "已暂停供数；本批及依赖它的放行批均暂扣 / suspended; "
                         "this batch and its dependent released batches are withheld"),
    }


async def resume_batch(pool: asyncpg.pool.Pool, *, batch_no: str,
                       actor: Optional[str] = None) -> Dict[str, Any]:
    """Lift one suspension; batches still blocked by other suspensions stay withheld.

    Single-connection throughout for the same no-self-deadlock guarantee as
    :func:`suspend_batch`.
    """
    actor = (actor or settings.instance_id).strip() or settings.instance_id
    async with pool.acquire() as conn:
        async with conn.transaction():
            batch = await _fetch_batch(conn, batch_no)
            if batch is None:
                raise SuspensionError(404,
                                      f"批号不存在 / batch not found: {batch_no}")
            await conn.execute("SELECT id FROM batches WHERE id = $1 FOR UPDATE",
                               batch["id"])
            existing = await conn.fetchrow(
                "SELECT reason FROM batch_suspensions WHERE batch_id = $1",
                batch["id"],
            )
            if existing is None:
                # Idempotent repeat of a resume (or a batch never suspended).
                await conn.execute(
                    "INSERT INTO batch_suspension_events"
                    "(batch_id, action, reason, actor) VALUES ($1,'RESUME',NULL,$2)",
                    batch["id"], actor)
                changed = False
            else:
                await conn.execute(
                    "DELETE FROM batch_suspensions WHERE batch_id = $1",
                    batch["id"])
                await conn.execute(
                    "INSERT INTO batch_suspension_events"
                    "(batch_id, action, reason, actor) VALUES ($1,'RESUME',NULL,$2)",
                    batch["id"], actor)
                changed = True
        # After commit, re-derive the global withheld set on this connection.
        withheld_ids = await _withheld_ids_conn(conn)
        still_withheld = batch["id"] in withheld_ids

    return {
        "batch_no": batch_no,
        "device_id": batch["device_id"],
        "suspended": False,
        "changed": changed,
        "still_withheld": still_withheld,
        "message": (
            "该批本未暂停，重复解除未改变状态 / was not suspended, no state change"
            if not changed
            else ("已解除暂停，但仍受其他暂停批影响而继续暂扣 / resumed but still "
                  "withheld due to another suspended batch"
                  if still_withheld
                  else "已解除暂停，供数资格恢复 / resumed, data supply restored")),
    }


async def list_withheld_batch_ids(pool: asyncpg.pool.Pool,
                                  root_id: Optional[int] = None) -> List[int]:
    """All currently withheld batch ids (or the sub-tree rooted at one batch)."""
    async with pool.acquire() as conn:
        return await _withheld_ids_conn(conn, root_id)


async def get_withholding_info(pool: asyncpg.pool.Pool,
                               batch_id: int) -> Dict[str, Any]:
    """Withholding state for batch detail: own suspension plus every source."""
    async with pool.acquire() as conn:
        own = await conn.fetchrow(
            "SELECT s.reason, s.created_at, s.created_by "
            "FROM batch_suspensions s WHERE s.batch_id = $1",
            batch_id,
        )
        # Every currently-suspended batch from which this batch is reachable
        # through the reference graph (it is always reachable from itself, so a
        # directly suspended batch lists itself as a source).
        sources = await conn.fetch(
            """
            WITH RECURSIVE reach(root_id, id) AS (
                SELECT batch_id, batch_id FROM batch_suspensions
                UNION
                SELECT r.root_id, b.id
                  FROM batches b
                  JOIN reach r
                    ON COALESCE(b.reference_ids, '[]'::jsonb) @> to_jsonb(r.id)
                 WHERE b.status = 'RELEASED'
            )
                SELECT DISTINCT s.batch_id, b.batch_no, s.reason,
                   s.created_at, s.created_by,
                   (s.batch_id = $1) AS direct
              FROM reach r
              JOIN batch_suspensions s ON s.batch_id = r.root_id
              JOIN batches b ON b.id = s.batch_id
             WHERE r.id = $1
             ORDER BY (s.batch_id = $1) DESC, s.created_at
            """,
            batch_id,
        )
        events = await conn.fetch(
            "SELECT action, reason, actor, created_at "
            "FROM batch_suspension_events WHERE batch_id = $1 ORDER BY id",
            batch_id,
        )
    return {
        "withheld": bool(own is not None or sources),
        "suspension": (
            None if own is None
            else {
                "reason": own["reason"],
                "suspended_at": own["created_at"].isoformat(),
                "suspended_by": own["created_by"],
            }
        ),
        "withholding_sources": [
            {
                "batch_id": r["batch_id"],
                "batch_no": r["batch_no"],
                "reason": r["reason"],
                "direct": r["direct"],
                "suspended_at": r["created_at"].isoformat(),
                "suspended_by": r["created_by"],
            }
            for r in sources
        ],
        "suspension_events": [
            {
                "action": e["action"],
                "reason": e["reason"],
                "actor": e["actor"],
                "created_at": e["created_at"].isoformat(),
            }
            for e in events
        ],
    }


async def list_suspensions(pool: asyncpg.pool.Pool) -> List[Dict[str, Any]]:
    """All currently active suspensions with their full withheld sub-trees."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT s.batch_id, b.batch_no, b.device_id, s.reason,
                   s.created_at, s.created_by
              FROM batch_suspensions s
              JOIN batches b ON b.id = s.batch_id
             ORDER BY s.created_at, s.batch_id
            """
        )
        result: List[Dict[str, Any]] = []
        for r in rows:
            withheld_nos = await conn.fetch(
                """
                WITH RECURSIVE sub(id) AS (
                    SELECT $1::bigint
                    UNION
                    SELECT b.id FROM batches b JOIN sub s
                      ON COALESCE(b.reference_ids, '[]'::jsonb) @> to_jsonb(s.id)
                     WHERE b.status='RELEASED'
                )
                SELECT b.batch_no FROM sub x JOIN batches b ON b.id = x.id
                 WHERE b.status='RELEASED' ORDER BY b.batch_no
                """,
                r["batch_id"],
            )
            result.append({
                "batch_id": r["batch_id"],
                "batch_no": r["batch_no"],
                "device_id": r["device_id"],
                "reason": r["reason"],
                "suspended_at": r["created_at"].isoformat(),
                "suspended_by": r["created_by"],
                "withheld_batch_nos": [x["batch_no"] for x in withheld_nos],
            })
        return result


def released_base_where(device_id: Optional[str],
                        batch_no: Optional[str]) -> Tuple[str, List[Any]]:
    where = ["b.status = 'RELEASED'", "b.id NOT IN (SELECT id FROM withheld)"]
    params: List[Any] = []
    if device_id:
        params.append(device_id)
        where.append(f"b.device_id = ${len(params)}")
    if batch_no:
        params.append(batch_no)
        where.append(f"b.batch_no = ${len(params)}")
    return " WHERE " + " AND ".join(where), params
