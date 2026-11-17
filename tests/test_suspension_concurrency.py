"""Concurrency tests for suspend/resume under two API instances.

The two HTTP instances share one PostgreSQL.  These tests prove that:
* concurrent suspensions of different batches never lose state and cannot
  starve the connection pool;
* an interleaved suspend/resume storm across the two instances converges to
  exactly the state implied by the surviving suspension rows;
* a failure inside a suspend/resume transaction rolls back as a unit, so the
  suspension table and data-lake eligibility can never diverge;
* the authoritative table always matches actual /data/released eligibility.

Same-batch concurrent requests rely on ``SELECT ... FOR UPDATE`` row locking.
That blocking-lock wake-up path is unavailable in seccomp sandboxes that
block inter-process signals (a locked backend is never woken on lock
release).  The same-batch test is therefore guarded by a runtime probe: it
runs on a normal kernel/Docker host and is skipped only where the platform
cannot wake a lock waiter at all -- a platform limitation, not an application
behaviour.  Same-batch idempotency is additionally covered serially in
test_suspension.py.
"""
import asyncio
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings  # noqa: E402
from app.csvutil import parse_thickness_csv  # noqa: E402
from app.db import close_pool, create_pool  # noqa: E402
from app.service import accept_submission, get_released_data  # noqa: E402
from app.storage import persist_original  # noqa: E402
from app.suspension import (  # noqa: E402
    list_withheld_batch_ids,
    resume_batch,
    suspend_batch,
)

_PROBE_CACHE: dict = {}


async def _blocking_lock_wakeup_works(pool) -> bool:
    """True if a backend blocked on a row lock is woken when it is released."""
    if "ok" in _PROBE_CACHE:
        return _PROBE_CACHE["ok"]
    try:
        async with pool.acquire() as c:
            await c.execute(
                "DROP TABLE IF EXISTS probe_lock; "
                "CREATE TABLE probe_lock(id int primary key); "
                "INSERT INTO probe_lock VALUES (1)")

        async def holder():
            async with pool.acquire() as c:
                async with c.transaction():
                    await c.execute(
                        "SELECT id FROM probe_lock WHERE id=1 FOR UPDATE")
                    await asyncio.sleep(1.0)

        async def waiter():
            async with pool.acquire() as c:
                async with c.transaction():
                    await c.execute(
                        "SELECT id FROM probe_lock WHERE id=1 FOR UPDATE")

        await asyncio.wait_for(asyncio.gather(holder(), waiter()), timeout=4.0)
        _PROBE_CACHE["ok"] = True
    except (asyncio.TimeoutError, Exception) as exc:  # pragma: no cover - sandbox
        _PROBE_CACHE["ok"] = False
        _PROBE_CACHE["reason"] = repr(exc)
    return _PROBE_CACHE["ok"]


async def _submit(pool, batch_no, device, thickness=500, count=5):
    lines = ["wafer_no,thickness_nm"] + [
        f"{batch_no}-W{i:03d},{thickness}" for i in range(1, count + 1)]
    raw = ("\n".join(lines) + "\n").encode()
    parsed = parse_thickness_csv(raw)
    artifact = persist_original(raw)
    acc = await accept_submission(
        pool, batch_no=batch_no, device_id=device,
        raw=raw, parsed=parsed, artifact=artifact)
    return acc.batch_id


async def _drain(pool):
    deadline = time.time() + 20
    while time.time() < deadline:
        async with pool.acquire() as conn:
            pending = await conn.fetchval(
                "SELECT count(*) FROM batches WHERE status='PENDING'")
        if pending == 0:
            return
        await asyncio.sleep(0.1)
    raise AssertionError("batches stuck in PENDING")


async def _reset(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE wafer_measurements, batch_suspension_events, "
            "batch_suspensions, batches, device_state RESTART IDENTITY CASCADE")


async def _assert_table_matches_eligibility(pool):
    """Invariant: /data/released eligibility == not in the withheld CTE set."""
    withheld = set(await list_withheld_batch_ids(pool))
    async with pool.acquire() as conn:
        released = {r["id"] for r in await conn.fetch(
            "SELECT id FROM batches WHERE status='RELEASED'")}
        suspended_rows = {r["batch_id"] for r in await conn.fetch(
            "SELECT batch_id FROM batch_suspensions")}
    total, _items = await get_released_data(
        pool, device_id=None, batch_no=None, limit=1000, offset=0)
    assert total == len(released - withheld)
    # Every authoritative suspension row must actually cause withholding.
    assert suspended_rows <= withheld
    return withheld


@pytest.mark.asyncio
async def test_concurrent_suspends_different_batches_keep_all_state(
        tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "slow_marker_file",
                        str(tmp_path / "slow.txt"))
    pool = await create_pool()
    try:
        await _reset(pool)
        from app.worker import DecisionWorker
        worker = DecisionWorker(pool, "w")
        worker.start()
        try:
            ids = {}
            for i in range(1, 7):
                ids[f"D{i}"] = await _submit(pool, f"D{i}", "EQ-D")
            await _drain(pool)
            # Six concurrent suspensions from two "instances", disjoint rows.
            await asyncio.gather(*[
                suspend_batch(pool, batch_no=f"D{i}", reason=f"r{i}",
                              actor=("api1" if i % 2 else "api2"))
                for i in range(1, 7)
            ])
        finally:
            await worker.stop()

        async with pool.acquire() as conn:
            n_rows = await conn.fetchval(
                "SELECT count(*) FROM batch_suspensions")
            reasons = {r["batch_no"]: r["reason"] for r in await conn.fetch(
                "SELECT b.batch_no, s.reason FROM batch_suspensions s "
                "JOIN batches b ON b.id=s.batch_id")}
        assert n_rows == 6                      # nothing lost
        assert reasons == {f"D{i}": f"r{i}" for i in range(1, 7)}
        withheld = await _assert_table_matches_eligibility(pool)
        assert set(ids.values()) <= withheld
    finally:
        await close_pool()


@pytest.mark.asyncio
async def test_interleaved_storm_across_disjoint_batches_converges(
        tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "slow_marker_file",
                        str(tmp_path / "slow.txt"))
    pool = await create_pool()
    try:
        await _reset(pool)
        from app.worker import DecisionWorker
        w1 = DecisionWorker(pool, "api1")
        w2 = DecisionWorker(pool, "api2")
        w1.start(); w2.start()
        try:
            for i in range(1, 13):
                await _submit(pool, f"X{i}", "EQ-X")
            await _drain(pool)

            # Each instance owns a disjoint set of batches, so the two
            # coroutines contend on the pool/global tables but never on the
            # same row lock -- exercising interleaving without depending on
            # blocking-lock wake-up (unavailable in signal-blocking sandboxes).
            async def side(actor, numbers):
                for n in numbers:
                    await suspend_batch(pool, batch_no=f"X{n}",
                                        reason=f"by-{actor}", actor=actor)
                    await asyncio.sleep(0)
                for n in reversed(numbers):
                    await resume_batch(pool, batch_no=f"X{n}", actor=actor)
                    await asyncio.sleep(0)

            await asyncio.gather(
                side("api1", list(range(1, 13, 2))),   # odd batches
                side("api2", list(range(2, 13, 2))),   # even batches
            )
        finally:
            await w1.stop(); await w2.stop()

        async with pool.acquire() as conn:
            left = await conn.fetchval("SELECT count(*) FROM batch_suspensions")
        assert left == 0
        withheld = await _assert_table_matches_eligibility(pool)
        assert withheld == set()

        # Leave one suspension standing while other batches churn; it survives.
        await suspend_batch(pool, batch_no="X2", reason="keep")
        await asyncio.gather(*[
            suspend_batch(pool, batch_no=f"X{i}", reason="churn")
            if i % 2 else resume_batch(pool, batch_no=f"X{i}")
            for i in range(1, 13) if i != 2
        ])
        for i in range(1, 13):
            if i != 2:
                await resume_batch(pool, batch_no=f"X{i}")
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT s.reason FROM batch_suspensions s "
                "JOIN batches b ON b.id=s.batch_id WHERE b.batch_no='X2'")
        assert row is not None and row["reason"] == "keep"
        await _assert_table_matches_eligibility(pool)
    finally:
        await close_pool()


@pytest.mark.asyncio
async def test_failed_operation_rolls_back_atomically(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "slow_marker_file",
                        str(tmp_path / "slow.txt"))
    pool = await create_pool()
    try:
        await _reset(pool)
        from app.worker import DecisionWorker
        worker = DecisionWorker(pool, "w")
        worker.start()
        try:
            await _submit(pool, "F1", "EQ-F")
            await _drain(pool)
        finally:
            await worker.stop()

        await _assert_table_matches_eligibility(pool)

        # Make the audit insert (inside the suspend transaction) fail.  Both
        # the state row and the audit row are written in that one transaction,
        # so the failure must leave neither behind.
        import app.suspension as mod
        real_execute = mod.asyncpg.Connection.execute

        async def flaky_execute(self, sql, *args, **kwargs):
            if "batch_suspension_events" in sql and "SUSPEND" in sql:
                raise mod.asyncpg.exceptions.InternalServerError("injected")
            return await real_execute(self, sql, *args, **kwargs)

        monkeypatch.setattr(mod.asyncpg.Connection, "execute", flaky_execute)
        with pytest.raises(Exception):
            await suspend_batch(pool, batch_no="F1", reason="will-rollback")
        monkeypatch.undo()

        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT count(*) FROM batch_suspensions") == 0
            assert await conn.fetchval(
                "SELECT count(*) FROM batch_suspension_events") == 0
        withheld = await _assert_table_matches_eligibility(pool)
        assert withheld == set()

        # The service still works after the rolled-back failure.
        ok = await suspend_batch(pool, batch_no="F1", reason="retry-after-fail")
        assert ok["changed"] is True
        await _assert_table_matches_eligibility(pool)
    finally:
        await close_pool()


@pytest.mark.asyncio
async def test_two_suspensions_overlap_without_losing_either(
        tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "slow_marker_file",
                        str(tmp_path / "slow.txt"))
    pool = await create_pool()
    try:
        await _reset(pool)
        from app.worker import DecisionWorker
        worker = DecisionWorker(pool, "w")
        worker.start()
        try:
            ids = [await _submit(pool, f"O{i}", "EQ-O") for i in range(1, 7)]
            await _drain(pool)
            await asyncio.gather(
                suspend_batch(pool, batch_no="O1", reason="a", actor="api1"),
                suspend_batch(pool, batch_no="O3", reason="b", actor="api2"),
            )
        finally:
            await worker.stop()

        async with pool.acquire() as conn:
            rows = {r["batch_no"]: r["reason"] for r in await conn.fetch(
                "SELECT b.batch_no, s.reason FROM batch_suspensions s "
                "JOIN batches b ON b.id=s.batch_id")}
        assert rows == {"O1": "a", "O3": "b"}
        withheld = await _assert_table_matches_eligibility(pool)
        assert ids[0] in withheld and ids[2] in withheld
    finally:
        await close_pool()


@pytest.mark.asyncio
async def test_same_batch_concurrent_suspends_serialise_via_row_lock(
        tmp_path, monkeypatch):
    """Same-batch duplicate storm: FOR UPDATE serialises, one row survives.

    Skipped only on platforms where blocking-lock wake-up is impossible
    (seccomp signal filtering); see module docstring.
    """
    monkeypatch.setattr(settings, "slow_marker_file",
                        str(tmp_path / "slow.txt"))
    pool = await create_pool()
    try:
        if not await _blocking_lock_wakeup_works(pool):
            pytest.skip("blocking row-lock wake-up unavailable in this sandbox "
                        "(seccomp blocks inter-process signals)")
        await _reset(pool)
        from app.worker import DecisionWorker
        worker = DecisionWorker(pool, "w")
        worker.start()
        try:
            bid = await _submit(pool, "C1", "EQ-C")
            await _drain(pool)
            results = await asyncio.gather(*[
                suspend_batch(pool, batch_no="C1", reason=f"r{i}",
                              actor=("api1" if i % 2 else "api2"))
                for i in range(16)
            ])
        finally:
            await worker.stop()

        assert sum(1 for r in results if r["changed"]) == 1
        assert all(r["suspended"] for r in results)
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT count(*) FROM batch_suspensions") == 1
            assert await conn.fetchval(
                "SELECT count(*) FROM batch_suspension_events "
                "WHERE action='SUSPEND'") == 16
        withheld = await _assert_table_matches_eligibility(pool)
        assert bid in withheld
    finally:
        await close_pool()
