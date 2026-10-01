-- Job state (versioned, compare-and-set updates) and deliveries (one row per attempt).

CREATE TABLE jobs (
    id               UUID         PRIMARY KEY,
    queue            VARCHAR(64)  NOT NULL,
    type             VARCHAR(64)  NOT NULL,
    payload          JSONB        NOT NULL,
    state            VARCHAR(16)  NOT NULL,
    version          BIGINT       NOT NULL,
    attempts         INTEGER      NOT NULL DEFAULT 0,
    max_attempts     INTEGER      NOT NULL,
    timeout_ms       BIGINT       NOT NULL,
    idempotency_key  VARCHAR(255),
    created_at       TIMESTAMPTZ  NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL,
    ready_at         TIMESTAMPTZ,
    dispatched_at    TIMESTAMPTZ,
    next_attempt_at  TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    last_error       TEXT,
    result           JSONB,
    CONSTRAINT jobs_state_check CHECK (state IN
        ('PENDING', 'QUEUED', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'FAILED', 'DEAD_LETTER', 'CANCELLED')),
    CONSTRAINT jobs_version_check CHECK (version >= 0),
    CONSTRAINT jobs_attempts_check CHECK (attempts >= 0 AND max_attempts >= 1),
    CONSTRAINT jobs_timeout_check CHECK (timeout_ms > 0)
);

-- Idempotency: one job per key. PostgreSQL is authoritative; any cache in front of it is optional.
CREATE UNIQUE INDEX ux_jobs_idempotency_key ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Queue stats and reconciler scans.
CREATE INDEX ix_jobs_queue_state ON jobs (queue, state);
CREATE INDEX ix_jobs_retry_due ON jobs (next_attempt_at) WHERE state = 'RETRY_WAIT';
CREATE INDEX ix_jobs_pending ON jobs (created_at) WHERE state = 'PENDING';
CREATE INDEX ix_jobs_queued_dispatch ON jobs (dispatched_at) WHERE state = 'QUEUED';

CREATE TABLE deliveries (
    job_id          UUID         NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt         INTEGER      NOT NULL,
    lease_owner     VARCHAR(128) NOT NULL,
    lease_deadline  TIMESTAMPTZ  NOT NULL,
    ack_state       VARCHAR(16)  NOT NULL,
    queued_at       TIMESTAMPTZ,
    started_at      TIMESTAMPTZ  NOT NULL,
    heartbeat_at    TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    progress        INTEGER,
    error           TEXT,
    PRIMARY KEY (job_id, attempt),
    CONSTRAINT deliveries_ack_state_check CHECK (ack_state IN ('LEASED', 'ACKED', 'NACKED', 'EXPIRED', 'CANCELLED')),
    CONSTRAINT deliveries_attempt_check CHECK (attempt >= 1),
    CONSTRAINT deliveries_progress_check CHECK (progress IS NULL OR (progress BETWEEN 0 AND 100))
);

-- Expired-lease scan: only open deliveries matter.
CREATE INDEX ix_deliveries_open_lease ON deliveries (lease_deadline) WHERE ack_state = 'LEASED';
