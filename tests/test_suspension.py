"""Tests for data-supply suspension / resume.

Covers the full requirement set:
* only released batches can be suspended, a non-empty reason is recorded;
* the suspended batch and every batch that directly or transitively uses it as
  a decision reference disappear from /data/released;
* batches decided *after* the suspension are withheld automatically;
* resuming one batch restores supply unless another active suspension still
  covers it;
* repeated suspend/resume calls are idempotent and never lose state;
* suspension never alters the original decision, the reference window or the
* background worker;
* the batch detail endpoint exposes the withholding sources;
* total count and detail page of one /data/released call match one snapshot.
"""
import pytest

from tests.conftest import submit, uniform_csv, wait_status


async def _release_chain(ac, device, prefix, n, thickness=500):
    """Submit and wait for n released batches on one device."""
    nos = []
    for i in range(1, n + 1):
        no = f"{prefix}{i}"
        r = await submit(ac, no, device, uniform_csv(no.lower(), 5, thickness))
        assert r.status_code == 202, r.text
        await wait_status(ac, no, "RELEASED")
        nos.append(no)
    return nos


async def _released_nos(ac, **params):
    r = await ac.get("/data/released", params={"limit": 1000, **params})
    assert r.status_code == 200
    body = r.json()
    return body["total_released"], {i["batch_no"] for i in body["items"]}


@pytest.mark.asyncio
async def test_suspend_requires_released_batch_and_reason(client):
    ac = client
    nos = await _release_chain(ac, "EQ-S1", "S1", 4)

    # Missing/blank reason -> 422.
    r = await ac.post(f"/batches/{nos[0]}/suspend", json={"reason": "   "})
    assert r.status_code == 422
    r = await ac.post(f"/batches/{nos[0]}/suspend", json={})
    assert r.status_code == 422

    # Unknown batch -> 404.
    r = await ac.post("/batches/NOPE/suspend", json={"reason": "x"})
    assert r.status_code == 404

    # A quarantined batch cannot be suspended -> 409.
    for i in range(1, 4):
        await submit(ac, f"QBASE{i}", "EQ-S1Q", uniform_csv(f"qb{i}", 5, 500))
        await wait_status(ac, f"QBASE{i}", "RELEASED")
    await submit(ac, "QDRIFT", "EQ-S1Q", uniform_csv("qd", 5, 900))
    await wait_status(ac, "QDRIFT", "QUARANTINED")
    r = await ac.post("/batches/QDRIFT/suspend", json={"reason": "x"})
    assert r.status_code == 409
    r = await ac.post("/batches/QDRIFT/resume")
    assert r.status_code == 200  # resume of never-suspended batch is idempotent


@pytest.mark.asyncio
async def test_suspend_withholds_transitive_dependents(client):
    ac = client
    # Six released batches; from batch 4 each window holds the previous three.
    nos = await _release_chain(ac, "EQ-S2", "S2", 6)
    total, served = await _released_nos(ac)
    assert total == 6 and served == set(nos)

    # Suspend batch 2: B2 and every batch whose reference chain reaches it
    # (B4 refs 1,2,3; B5 refs 2,3,4; B6 refs 3,4,5 -> all of B4,B5,B6) go.
    r = await ac.post(f"/batches/{nos[1]}/suspend",
                      json={"reason": "原件待复核 / raw file re-check"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["changed"] is True
    assert body["suspended"] is True
    assert body["reason"] == "原件待复核 / raw file re-check"
    assert set(body["withheld_batch_nos"]) == {nos[1], nos[3], nos[4], nos[5]}

    total, served = await _released_nos(ac)
    assert total == 2
    assert served == {nos[0], nos[2]}  # only B1 and B3 remain eligible

    # Explicit filter on a withheld batch returns nothing.
    total, served = await _released_nos(ac, batch_no=nos[3])
    assert total == 0 and served == set()

    # Batch detail shows the withholding source chain.
    d = await ac.get(f"/batches/{nos[5]}")
    assert d.status_code == 200
    detail = d.json()
    assert detail["status"] == "RELEASED"          # decision itself untouched
    assert detail["withheld"] is True
    assert detail["suspension"] is None            # not directly suspended
    sources = {s["batch_no"] for s in detail["withholding_sources"]}
    assert sources == {nos[1]}

    # Directly suspended batch lists itself as the source with the reason.
    d0 = (await ac.get(f"/batches/{nos[1]}")).json()
    assert d0["withheld"] is True
    assert d0["suspension"]["reason"].startswith("原件待复核")
    assert {s["batch_no"] for s in d0["withholding_sources"]} == {nos[1]}
    assert d0["withholding_sources"][0]["direct"] is True
    assert any(e["action"] == "SUSPEND" for e in d0["suspension_events"])


@pytest.mark.asyncio
async def test_later_decided_dependent_is_withheld_automatically(client):
    ac = client
    nos = await _release_chain(ac, "EQ-S3", "S3", 3)
    # Suspend a baseline batch before batch 4 is even submitted.
    r = await ac.post(f"/batches/{nos[0]}/suspend", json={"reason": "r"})
    assert r.status_code == 200

    later = "S3LATER"
    await submit(ac, later, "EQ-S3", uniform_csv("later", 5, 500))
    await wait_status(ac, later, "RELEASED")

    # The newly decided batch references the suspended baseline (directly), so
    # it must be withheld without any extra operation.
    detail = (await ac.get(f"/batches/{later}")).json()
    assert detail["reference_batch_nos"] == nos
    assert detail["withheld"] is True
    total, served = await _released_nos(ac)
    assert later not in served
    # And the worker still recorded the decision/reference window normally.
    assert detail["status"] == "RELEASED" and detail["reference_batch_nos"]


@pytest.mark.asyncio
async def test_resume_restores_and_other_suspension_keeps_withholding(client):
    ac = client
    nos = await _release_chain(ac, "EQ-S4", "S4", 6)
    await ac.post(f"/batches/{nos[0]}/suspend", json={"reason": "r1"})
    await ac.post(f"/batches/{nos[2]}/suspend", json={"reason": "r3"})

    # B5 is reachable from both roots (B5 refs B2,B3,B4; B4 refs B1,B2,B3).
    detail5 = (await ac.get(f"/batches/{nos[4]}")).json()
    assert {s["batch_no"] for s in detail5["withholding_sources"]} == {
        nos[0], nos[2]}

    # Resume B1: B5 is still covered by the B3 suspension.
    r = await ac.post(f"/batches/{nos[0]}/resume")
    assert r.status_code == 200
    body = r.json()
    assert body["changed"] is True
    detail5 = (await ac.get(f"/batches/{nos[4]}")).json()
    assert detail5["withheld"] is True
    assert {s["batch_no"] for s in detail5["withholding_sources"]} == {nos[2]}

    # B1 itself is fully restored.
    detail1 = (await ac.get(f"/batches/{nos[0]}")).json()
    assert detail1["withheld"] is False and detail1["suspension"] is None
    total, served = await _released_nos(ac, batch_no=nos[0])
    assert total == 1

    # Resume B3 clears the last source; everything is served again.
    r = await ac.post(f"/batches/{nos[2]}/resume")
    assert r.json()["still_withheld"] is False
    total, served = await _released_nos(ac)
    assert total == 6 and served == set(nos)

    # The audit trail retains the full suspend/resume history.
    detail3 = (await ac.get(f"/batches/{nos[2]}")).json()
    actions = [e["action"] for e in detail3["suspension_events"]]
    assert actions == ["SUSPEND", "RESUME"]


@pytest.mark.asyncio
async def test_repeated_operations_are_idempotent_and_keep_state(client):
    ac = client
    nos = await _release_chain(ac, "EQ-S5", "S5", 4)
    target = nos[1]

    r1 = await ac.post(f"/batches/{target}/suspend", json={"reason": "first"})
    r2 = await ac.post(f"/batches/{target}/suspend", json={"reason": "second"})
    assert r1.json()["changed"] is True
    assert r2.json()["changed"] is False
    # Original reason/state is preserved; the duplicate is only audited.
    assert r2.json()["reason"] == "first"

    r3 = await ac.post(f"/batches/{target}/resume")
    r4 = await ac.post(f"/batches/{target}/resume")
    assert r3.json()["changed"] is True
    assert r4.json()["changed"] is False

    detail = (await ac.get(f"/batches/{target}")).json()
    assert detail["withheld"] is False
    assert [e["action"] for e in detail["suspension_events"]] == [
        "SUSPEND", "SUSPEND", "RESUME", "RESUME"]
    # Exactly one suspension row ever existed at a time.
    r = await ac.get("/suspensions")
    assert r.status_code == 200
    assert r.json()["count"] == 0


@pytest.mark.asyncio
async def test_suspension_does_not_change_decision_or_reference_window(client):
    ac = client
    before = await _release_chain(ac, "EQ-S6", "S6", 5)
    detail_before = (await ac.get(f"/batches/{before[4]}")).json()
    assert detail_before["reference_batch_nos"] == before[1:4]  # S62,S63,S64

    await ac.post(f"/batches/{before[1]}/suspend", json={"reason": "r"})
    # A new batch (the 6th) is still decided normally against the rolling
    # window of the three most recent released batches: S63,S64,S65.
    new_no = "S6NEW"
    await submit(ac, new_no, "EQ-S6", uniform_csv("new", 5, 502))
    decided = await wait_status(ac, new_no, "RELEASED")
    assert decided["status"] == "RELEASED"
    assert decided["reference_batch_nos"] == before[2:5]  # S63,S64,S65
    # The worker still recorded a normal decision even though it is withheld.
    assert decided["withheld"] is True

    # The earlier batch's stored decision fields are byte-for-byte unchanged.
    detail_after = (await ac.get(f"/batches/{before[4]}")).json()
    for key in ("status", "median_nm", "reference_mean_nm", "deviation_nm",
                "reference_batch_ids", "reference_batch_nos"):
        assert detail_after[key] == detail_before[key]


@pytest.mark.asyncio
async def test_suspensions_listing_reports_subtrees(client):
    ac = client
    nos = await _release_chain(ac, "EQ-S7", "S7", 5)
    await ac.post(f"/batches/{nos[0]}/suspend", json={"reason": "root"})
    r = await ac.get("/suspensions")
    assert r.status_code == 200
    rows = r.json()["suspensions"]
    assert len(rows) == 1
    row = rows[0]
    assert row["batch_no"] == nos[0]
    assert row["reason"] == "root"
    # B1 reaches B4 (refs 1,2,3) and B5 (refs 2,3,4) transitively.
    assert set(row["withheld_batch_nos"]) == {nos[0], nos[3], nos[4]}


@pytest.mark.asyncio
async def test_released_count_and_details_share_one_snapshot(client):
    ac = client
    nos = await _release_chain(ac, "EQ-S8", "S8", 4)
    await ac.post(f"/batches/{nos[0]}/suspend", json={"reason": "r"})

    r = await ac.get("/data/released", params={"limit": 1000})
    body = r.json()
    # The page length must always equal the reported total for this snapshot.
    assert body["total_released"] == len(body["items"])
    assert body["total_released"] == 2
    # Pagination stays internally consistent under withholding.
    page1 = (await ac.get("/data/released",
                          params={"limit": 1, "offset": 0})).json()
    page2 = (await ac.get("/data/released",
                          params={"limit": 1, "offset": 1})).json()
    assert page1["total_released"] == page2["total_released"] == 2
    assert page1["items"][0]["batch_no"] != page2["items"][0]["batch_no"]
    page3 = (await ac.get("/data/released",
                          params={"limit": 1, "offset": 2})).json()
    assert page3["items"] == []
