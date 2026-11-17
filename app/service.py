"""Submission/acceptance service and read models."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from .csvutil import ParsedCsv
from .storage import StoredArtifact
from .suspension import get_withholding_info, released_base_where

# Unique constraint backing per-device accept_seq (see schema.py).  Concurrent
# inserts of the same next sequence collide here and are retried, which keeps
# every intake transaction short -- no connection is ever held while blocked
# on a lock, so the pool cannot be starved by same-device contention.
DEVICE_SEQ_CONSTRAINT = "batches_device_accept_uniq"
BATCH_NO_CONSTRAINT = "batches_batch_no_uniq"


class ConflictError(Exception):
    """HTTP 409: duplicate batch number with conflicting content/device."""

    def __init__(self, message: str, existing: Dict[str, Any]):
        super().__init__(message)
        self.existing = existing


@dataclass
class Acceptance:
    batch_id: int
    batch_no: str
    device_id: str
    accept_seq: int
    status: str
    reused: bool


async def accept_submission(
    pool: asyncpg.pool.Pool,
    *,
    batch_no: str,
    device_id: str,
    raw: bytes,
    parsed: ParsedCsv,
    artifact: StoredArtifact,
) -> Acceptance:
    """Persist a new batch or resolve an idempotent retry.

    The original file is already on disk (see :mod:`app.storage`) before this
    runs.  Same ``batch_no`` + same device + identical bytes reuses the
    existing row; any other combination of an existing number is rejected.
    """
    for attempt in range(50):
        try:
            return await _accept_once(
                pool, batch_no=batch_no, device_id=device_id,
                parsed=parsed, artifact=artifact)
        except asyncpg.UniqueViolationError as exc:
            if exc.constraint_name == DEVICE_SEQ_CONSTRAINT:
                # Another same-device submission won this sequence number;
                # re-read the max and try again. Tiny bounded backoff.
                await asyncio.sleep(min(0.05, 0.005 * (attempt + 1)))
                continue
            if exc.constraint_name == BATCH_NO_CONSTRAINT:
                # Race: two brand-new submissions with the same batch_no.
                # Re-read and turn it into the normal 409 decision.
                async with pool.acquire() as conn:
                    existing = await conn.fetchrow(
                        "SELECT id, batch_no, device_id, sha256, accept_seq, "
                        "status FROM batches WHERE batch_no = $1", batch_no)
                if existing is not None:
                    return _reuse_or_conflict(batch_no, device_id, artifact, existing)
            raise
    raise RuntimeError(f"could not allocate accept_seq for {batch_no} after retries")


async def _accept_once(pool, *, batch_no, device_id, parsed, artifact) -> Acceptance:
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT id, batch_no, device_id, sha256, accept_seq, status "
                "FROM batches WHERE batch_no = $1",
                batch_no,
            )
            if existing is not None:
                return _reuse_or_conflict(batch_no, device_id, artifact, existing)

            next_seq = await conn.fetchval(
                "SELECT COALESCE(MAX(accept_seq), 0) + 1 FROM batches "
                "WHERE device_id = $1",
                device_id,
            )
            batch_id = await conn.fetchval(
                """
                INSERT INTO batches
                    (batch_no, device_id, sha256, size_bytes, storage_path,
                     accept_seq, status)
                VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')
                RETURNING id
                """,
                batch_no, device_id, artifact.sha256, artifact.size_bytes,
                artifact.storage_path, next_seq,
            )
            await conn.executemany(
                "INSERT INTO wafer_measurements(batch_id, wafer_no, thickness_nm) "
                "VALUES ($1, $2, $3)",
                [(batch_id, w, t) for w, t in parsed.rows],
            )
            await conn.execute(
                "INSERT INTO device_state(device_id) VALUES ($1) "
                "ON CONFLICT (device_id) DO NOTHING",
                device_id,
            )
            return Acceptance(
                batch_id=batch_id, batch_no=batch_no, device_id=device_id,
                accept_seq=next_seq, status="PENDING", reused=False,
            )


def _reuse_or_conflict(batch_no: str, device_id: str,
                       artifact: StoredArtifact,
                       existing: asyncpg.Record) -> Acceptance:
    """Idempotent reuse on identical re-submission, else HTTP 409."""
    if existing["device_id"] != device_id:
        raise ConflictError(
            f"批号 {batch_no} 已被其他设备占用 / batch number already "
            "registered for another device", _brief(existing))
    if existing["sha256"] != artifact.sha256:
        raise ConflictError(
            f"批号 {batch_no} 已存在但 CSV 内容不同（sha256 不一致），"
            "拒绝重复提交 / same batch number with different content",
            _brief(existing))
    # Identical retry: re-use the original acceptance.
    return Acceptance(
        batch_id=existing["id"], batch_no=batch_no, device_id=device_id,
        accept_seq=existing["accept_seq"], status=existing["status"],
        reused=True,
    )


def _brief(row: asyncpg.Record) -> Dict[str, Any]:
    return {
        "batch_no": row["batch_no"],
        "device_id": row["device_id"],
        "accept_seq": row["accept_seq"],
        "status": row["status"],
    }


async def get_batch_detail(pool: asyncpg.pool.Pool,
                           batch_no: str) -> Optional[Dict[str, Any]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM batches WHERE batch_no = $1", batch_no)
        if row is None:
            return None
        ref_ids: List[int] = list(row["reference_ids"] or [])
        ref_batch_nos: List[str] = []
        if ref_ids:
            ref_rows = await conn.fetch(
                "SELECT id, batch_no FROM batches WHERE id = ANY($1::bigint[]) "
                "ORDER BY array_position($2::bigint[], id)",
                ref_ids, ref_ids,
            )
            ref_batch_nos = [r["batch_no"] for r in ref_rows]
        detail = {
            "batch_no": row["batch_no"],
            "device_id": row["device_id"],
            "status": row["status"],
            "accept_seq": row["accept_seq"],
            "wafer_count": await conn.fetchval(
                "SELECT count(*) FROM wafer_measurements WHERE batch_id=$1",
                row["id"]),
            "median_nm": row["median_nm"],
            "reference_mean_nm": row["ref_mean_nm"],
            "deviation_nm": row["deviation_nm"],
            "reference_batch_ids": ref_ids,
            "reference_batch_nos": ref_batch_nos,
            "sha256": row["sha256"],
            "original_storage_path": row["storage_path"],
            "attempts": row["attempts"],
            "decided_by": row["decided_by"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "decided_at": row["decided_at"].isoformat() if row["decided_at"] else None,
        }
    # Suspension/withholding state (released batches only can ever be withheld;
    # pending and quarantined batches never enter the data lake anyway).
    if row["status"] == "RELEASED":
        detail.update(await get_withholding_info(pool, row["id"]))
    else:
        detail.update({"withheld": False, "suspension": None,
                       "withholding_sources": [], "suspension_events": []})
    return detail


async def list_device_batches(pool: asyncpg.pool.Pool,
                              device_id: str) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT batch_no, accept_seq, status, median_nm, deviation_nm, "
            "decided_at, attempts FROM batches WHERE device_id=$1 "
            "ORDER BY accept_seq",
            device_id,
        )
        return [
            {
                "batch_no": r["batch_no"],
                "accept_seq": r["accept_seq"],
                "status": r["status"],
                "median_nm": r["median_nm"],
                "deviation_nm": r["deviation_nm"],
                "attempts": r["attempts"],
                "decided_at": r["decided_at"].isoformat() if r["decided_at"] else None,
            }
            for r in rows
        ]


async def get_released_data(pool: asyncpg.pool.Pool, *,
                            device_id: Optional[str],
                            batch_no: Optional[str],
                            limit: int,
                            offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    """Read model for the data lake: only *eligible* released batches show up.

    A released batch is eligible when it is neither suspended itself nor
    reachable (directly or transitively through ``reference_ids``) from a
    suspended batch.  The count and the page of details are read inside one
    REPEATABLE READ transaction, so both always correspond to the same
    committed state even if a suspend/resume lands between the two statements.
    """
    from .suspension import _WITHHELD_CTE

    where_sql, params = released_base_where(device_id, batch_no)

    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read",
                                    readonly=True):
            total = await conn.fetchval(
                _WITHHELD_CTE
                + "SELECT count(*) FROM batches b" + where_sql,
                *params,
            )
            page_params = params + [limit, offset]
            rows = await conn.fetch(
                _WITHHELD_CTE
                + "SELECT b.id, b.batch_no, b.device_id, b.accept_seq, "
                "b.median_nm, b.decided_at FROM batches b" + where_sql
                + f" ORDER BY b.id LIMIT ${len(params) + 1}"
                f" OFFSET ${len(params) + 2}",
                *page_params,
            )
            out: List[Dict[str, Any]] = []
            for r in rows:
                wafers = await conn.fetch(
                    "SELECT wafer_no, thickness_nm FROM wafer_measurements "
                    "WHERE batch_id=$1 ORDER BY wafer_no", r["id"])
                out.append({
                    "batch_no": r["batch_no"],
                    "device_id": r["device_id"],
                    "accept_seq": r["accept_seq"],
                    "median_nm": r["median_nm"],
                    "decided_at": r["decided_at"].isoformat() if r["decided_at"] else None,
                    "wafers": [
                        {"wafer_no": w["wafer_no"], "thickness_nm": w["thickness_nm"]}
                        for w in wafers
                    ],
                })
            return total, out
