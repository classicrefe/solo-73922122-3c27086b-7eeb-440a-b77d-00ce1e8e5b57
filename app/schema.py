"""PostgreSQL schema definition and initialisation.

Durability model
----------------
* ``batches`` carries every persisted fact about a submission and its
  decision.  Wafers are stored in a child table ``wafer_measurements``.
* A batch is processed inside a *single* transaction that inserts the
  decision and either appends the batch id to ``device_state.ref_batch_ids``
  (released only) or leaves it untouched.  Hence the decision, the reference
  history and data-lake visibility become visible atomically -- there is no
  observable intermediate ("half batch") state and the reference list can
  never be advanced twice.
* Row-level locking (``SELECT ... FOR UPDATE`` on ``device_state`` plus a
  transaction-scoped advisory lock on the device) serialises workers for one
  device while different devices proceed independently.

Suspension model
----------------
* ``batch_suspensions`` holds one row per currently-suspended released batch
  (primary key on batch id); ``batch_suspension_events`` is the append-only
  audit trail that survives a later resume.
* Suspension never alters a decision, the reference window or background
  processing.  The set of withheld batches -- the suspended batches plus
  every released batch that directly or transitively used one as a decision
  reference -- is computed on read with a recursive CTE over
  ``batches.reference_ids``, so batches decided later are withheld
  automatically without any write.
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS batches (
    id              BIGGENERATED_PLACEHOLDER,
    batch_no        TEXT        NOT NULL,
    device_id       TEXT        NOT NULL,
    sha256          TEXT        NOT NULL,
    size_bytes      BIGINT      NOT NULL,
    storage_path    TEXT        NOT NULL,
    accept_seq      BIGINT      NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('PENDING','RELEASED','QUARANTINED')),
    median_nm       DOUBLE PRECISION,
    ref_mean_nm     DOUBLE PRECISION,
    deviation_nm    DOUBLE PRECISION,
    reference_ids   JSONB,
    decided_at      TIMESTAMPTZ,
    decided_by      TEXT,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT batches_pkey PRIMARY KEY (id),
    CONSTRAINT batches_batch_no_uniq UNIQUE (batch_no),
    CONSTRAINT batches_device_accept_uniq UNIQUE (device_id, accept_seq)
);

CREATE TABLE IF NOT EXISTS wafer_measurements (
    batch_id    BIGINT  NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    wafer_no    TEXT    NOT NULL,
    thickness_nm INTEGER NOT NULL CHECK (thickness_nm > 0),
    CONSTRAINT wafer_measurements_pkey PRIMARY KEY (batch_id, wafer_no)
);

CREATE INDEX IF NOT EXISTS idx_batches_device_seq
    ON batches (device_id, accept_seq);
CREATE INDEX IF NOT EXISTS idx_batches_status
    ON batches (status) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_wafers_batch ON wafer_measurements (batch_id);
-- Supports the recursive dependency walk used by withholding: find every
-- batch whose reference window contains a given batch id.
CREATE INDEX IF NOT EXISTS idx_batches_reference_ids
    ON batches USING GIN (reference_ids);

-- One row per device: the durable processing cursor (last accepted sequence
-- that has been decided) and the FIFO list of released batch ids that form the
-- rolling reference window (most recent last).
CREATE TABLE IF NOT EXISTS device_state (
    device_id        TEXT PRIMARY KEY,
    last_decided_seq BIGINT NOT NULL DEFAULT 0,
    ref_batch_ids    JSONB  NOT NULL DEFAULT '[]'::jsonb,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Data-supply suspension state.  A row exists exactly while a released batch
-- is suspended; the primary key makes repeated suspends idempotent under
-- concurrent requests from the two API instances.  Suspension never changes
-- the batch decision itself -- status stays RELEASED, only visibility in
-- /data/released is withheld (for this row and every batch that directly or
-- transitively references it, computed on read via recursive CTE).
CREATE TABLE IF NOT EXISTS batch_suspensions (
    batch_id    BIGINT PRIMARY KEY
                REFERENCES batches(id) ON DELETE RESTRICT,
    reason      TEXT        NOT NULL CHECK (length(btrim(reason)) > 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by  TEXT        NOT NULL
);

-- Append-only audit trail of every suspend/resume operation, including
-- duplicate/idempotent calls, so the history survives a later resume.
CREATE TABLE IF NOT EXISTS batch_suspension_events (
    id          BIGGENERATED_PLACEHOLDER,
    batch_id    BIGINT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    action      TEXT   NOT NULL CHECK (action IN ('SUSPEND','RESUME')),
    reason      TEXT,
    actor       TEXT   NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT batch_suspension_events_pkey PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_suspension_events_batch
    ON batch_suspension_events (batch_id, id);
"""


async def init_schema(conn) -> None:
    # BIGSERIAL spelled out so the file stays readable on its own.
    sql = SCHEMA_SQL.replace("BIGGENERATED_PLACEHOLDER",
                             "BIGINT GENERATED ALWAYS AS IDENTITY")
    await conn.execute(sql)
