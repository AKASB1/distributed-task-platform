package io.akasb.taskplatform.persistence;

import io.akasb.taskplatform.domain.AckState;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * PostgreSQL implementation. Single-row changes are one {@code UPDATE ... WHERE id = ? AND version = ?}; changes that
 * touch a job and its delivery run in one transaction, job row first, so lock order is always job then delivery.
 */
public final class JdbcJobRepository implements JobRepository {

    private static final String JOB_COLUMNS = """
            id, queue, type, payload::text AS payload, state, version, attempts, max_attempts, timeout_ms,
            idempotency_key, created_at, updated_at, ready_at, dispatched_at, next_attempt_at, finished_at,
            last_error, result::text AS result""";

    private static final String DELIVERY_COLUMNS = """
            job_id, attempt, lease_owner, lease_deadline, ack_state, queued_at, started_at, heartbeat_at,
            finished_at, progress, error""";

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;

    public JdbcJobRepository(DataSource dataSource) {
        this.jdbc = JdbcClient.create(dataSource);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @Override
    public InsertResult insert(Job job) {
        int inserted = jdbc.sql("""
                        INSERT INTO jobs (id, queue, type, payload, state, version, attempts, max_attempts, timeout_ms,
                                          idempotency_key, created_at, updated_at, ready_at, dispatched_at,
                                          next_attempt_at, finished_at, last_error, result)
                        VALUES (:id, :queue, :type, CAST(:payload AS jsonb), :state, :version, :attempts, :maxAttempts,
                                :timeoutMs, :idempotencyKey, :createdAt, :updatedAt, :readyAt, :dispatchedAt,
                                :nextAttemptAt, :finishedAt, :lastError, CAST(:result AS jsonb))
                        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING""")
                .paramSource(jobParams(job))
                .update();
        if (inserted == 1) return new InsertResult(job, true);
        Job existing = findByIdempotencyKey(job.idempotencyKey())
                .orElseThrow(() -> new IllegalStateException("idempotency conflict but no job for key"));
        return new InsertResult(existing, false);
    }

    @Override
    public Optional<Job> find(UUID id) {
        return jdbc.sql("SELECT " + JOB_COLUMNS + " FROM jobs WHERE id = :id")
                .param("id", id)
                .query(JOB_MAPPER)
                .optional();
    }

    @Override
    public Optional<Job> findByIdempotencyKey(String idempotencyKey) {
        return jdbc.sql("SELECT " + JOB_COLUMNS + " FROM jobs WHERE idempotency_key = :key")
                .param("key", idempotencyKey)
                .query(JOB_MAPPER)
                .optional();
    }

    @Override
    public boolean update(Job next, long expectedVersion) {
        return updateJob(next, expectedVersion) == 1;
    }

    @Override
    public boolean claim(Job running, long expectedVersion, Delivery delivery) {
        try {
            return Boolean.TRUE.equals(tx.execute(status -> {
                if (updateJob(running, expectedVersion) != 1) return false;
                jdbc.sql("INSERT INTO deliveries (" + DELIVERY_COLUMNS + ") VALUES (:jobId, :attempt, :leaseOwner,"
                                + " :leaseDeadline, :ackState, :queuedAt, :startedAt, :heartbeatAt, :finishedAt,"
                                + " :progress, :error)")
                        .paramSource(deliveryParams(delivery))
                        .update();
                return true;
            }));
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    @Override
    public boolean finishAttempt(Job next, long expectedVersion, Delivery closed, Instant leaseExpiredBefore) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            if (updateJob(next, expectedVersion) != 1) return false;
            if (closeOpenDelivery(closed, leaseExpiredBefore) != 1) {
                status.setRollbackOnly();
                return false;
            }
            return true;
        }));
    }

    @Override
    public boolean renewLease(UUID jobId, int attempt, String owner, long jobVersion, Instant newDeadline,
                              Instant heartbeatAt, Integer progress) {
        return jdbc.sql("""
                        UPDATE deliveries d
                           SET lease_deadline = :deadline, heartbeat_at = :heartbeatAt,
                               progress = COALESCE(:progress, d.progress)
                          FROM jobs j
                         WHERE d.job_id = :jobId AND d.attempt = :attempt AND d.lease_owner = :owner
                           AND d.ack_state = 'LEASED'
                           AND j.id = d.job_id AND j.version = :version AND j.state = 'RUNNING'""")
                .param("deadline", ts(newDeadline))
                .param("heartbeatAt", ts(heartbeatAt))
                .param("progress", progress, Types.INTEGER)
                .param("jobId", jobId)
                .param("attempt", attempt)
                .param("owner", owner)
                .param("version", jobVersion)
                .update() == 1;
    }

    @Override
    public boolean closeDelivery(Delivery closed, Instant leaseExpiredBefore) {
        return closeOpenDelivery(closed, leaseExpiredBefore) == 1;
    }

    @Override
    public void markDispatched(UUID id, Instant at) {
        jdbc.sql("UPDATE jobs SET dispatched_at = :at WHERE id = :id AND state = 'QUEUED'")
                .param("at", ts(at))
                .param("id", id)
                .update();
    }

    @Override
    public List<Delivery> deliveries(UUID jobId) {
        return jdbc.sql("SELECT " + DELIVERY_COLUMNS + " FROM deliveries WHERE job_id = :jobId ORDER BY attempt")
                .param("jobId", jobId)
                .query(DELIVERY_MAPPER)
                .list();
    }

    @Override
    public List<Delivery> findExpiredLeases(Instant before, int limit) {
        return jdbc.sql("SELECT " + DELIVERY_COLUMNS + " FROM deliveries WHERE ack_state = 'LEASED'"
                        + " AND lease_deadline < :before ORDER BY lease_deadline LIMIT :limit")
                .param("before", ts(before))
                .param("limit", limit)
                .query(DELIVERY_MAPPER)
                .list();
    }

    @Override
    public List<Job> findDueRetries(Instant now, int limit) {
        return jdbc.sql("SELECT " + JOB_COLUMNS + " FROM jobs WHERE state = 'RETRY_WAIT'"
                        + " AND next_attempt_at <= :now ORDER BY next_attempt_at LIMIT :limit")
                .param("now", ts(now))
                .param("limit", limit)
                .query(JOB_MAPPER)
                .list();
    }

    @Override
    public List<Job> findStalePending(Instant before, int limit) {
        return jdbc.sql("SELECT " + JOB_COLUMNS + " FROM jobs WHERE state = 'PENDING'"
                        + " AND created_at < :before ORDER BY created_at LIMIT :limit")
                .param("before", ts(before))
                .param("limit", limit)
                .query(JOB_MAPPER)
                .list();
    }

    @Override
    public List<Job> findQueuedDispatchedBefore(Instant before, int limit) {
        // Two range scans on the partial index ix_jobs_queued_dispatch instead of an OR + NULLS FIRST sort, which
        // would visit every QUEUED row on each reconciler pass when the backlog is large.
        List<Job> result = new java.util.ArrayList<>(jdbc.sql("SELECT " + JOB_COLUMNS + " FROM jobs"
                        + " WHERE state = 'QUEUED' AND dispatched_at IS NULL LIMIT :limit")
                .param("limit", limit)
                .query(JOB_MAPPER)
                .list());
        if (result.size() < limit) {
            result.addAll(jdbc.sql("SELECT " + JOB_COLUMNS + " FROM jobs WHERE state = 'QUEUED'"
                            + " AND dispatched_at < :before ORDER BY dispatched_at LIMIT :limit")
                    .param("before", ts(before))
                    .param("limit", limit - result.size())
                    .query(JOB_MAPPER)
                    .list());
        }
        return result;
    }

    @Override
    public QueueStats queueStats(String queue, Instant now) {
        Map<JobState, Long> counts = new EnumMap<>(JobState.class);
        jdbc.sql("SELECT state, count(*) AS n FROM jobs WHERE queue = :queue GROUP BY state")
                .param("queue", queue)
                .query((rs, i) -> Map.entry(JobState.valueOf(rs.getString("state")), rs.getLong("n")))
                .list()
                .forEach(e -> counts.put(e.getKey(), e.getValue()));
        Instant oldest = jdbc.sql("SELECT min(ready_at) FROM jobs WHERE queue = :queue AND state = 'QUEUED'")
                .param("queue", queue)
                .query((rs, i) -> instant(rs, 1))
                .optional()
                .orElse(null);
        return new QueueStats(queue, counts, oldest, now);
    }

    @Override
    public Map<JobState, Long> countByState() {
        Map<JobState, Long> counts = new EnumMap<>(JobState.class);
        jdbc.sql("SELECT state, count(*) AS n FROM jobs GROUP BY state")
                .query((rs, i) -> Map.entry(JobState.valueOf(rs.getString("state")), rs.getLong("n")))
                .list()
                .forEach(e -> counts.put(e.getKey(), e.getValue()));
        return counts;
    }

    private int updateJob(Job next, long expectedVersion) {
        return jdbc.sql("""
                        UPDATE jobs
                           SET state = :state, version = :version, attempts = :attempts, updated_at = :updatedAt,
                               ready_at = :readyAt, dispatched_at = :dispatchedAt, next_attempt_at = :nextAttemptAt,
                               finished_at = :finishedAt, last_error = :lastError, result = CAST(:result AS jsonb)
                         WHERE id = :id AND version = :expectedVersion""")
                .paramSource(jobParams(next).addValue("expectedVersion", expectedVersion))
                .update();
    }

    private int closeOpenDelivery(Delivery closed, Instant leaseExpiredBefore) {
        MapSqlParameterSource params = deliveryParams(closed);
        String expiryGuard = "";
        if (leaseExpiredBefore != null) {
            expiryGuard = " AND lease_deadline < :expiredBefore";
            params.addValue("expiredBefore", ts(leaseExpiredBefore));
        }
        return jdbc.sql("UPDATE deliveries SET ack_state = :ackState, finished_at = :finishedAt, error = :error,"
                        + " heartbeat_at = :heartbeatAt, progress = :progress"
                        + " WHERE job_id = :jobId AND attempt = :attempt AND lease_owner = :leaseOwner"
                        + " AND ack_state = 'LEASED'" + expiryGuard)
                .paramSource(params)
                .update();
    }

    private static MapSqlParameterSource jobParams(Job job) {
        var p = new MapSqlParameterSource();
        p.addValue("id", job.id());
        p.addValue("queue", job.queue());
        p.addValue("type", job.type());
        p.addValue("payload", job.payload());
        p.addValue("state", job.state().name());
        p.addValue("version", job.version());
        p.addValue("attempts", job.attempts());
        p.addValue("maxAttempts", job.maxAttempts());
        p.addValue("timeoutMs", job.timeout().toMillis());
        p.addValue("idempotencyKey", job.idempotencyKey(), Types.VARCHAR);
        p.addValue("createdAt", ts(job.createdAt()));
        p.addValue("updatedAt", ts(job.updatedAt()));
        p.addValue("readyAt", ts(job.readyAt()), Types.TIMESTAMP_WITH_TIMEZONE);
        p.addValue("dispatchedAt", ts(job.dispatchedAt()), Types.TIMESTAMP_WITH_TIMEZONE);
        p.addValue("nextAttemptAt", ts(job.nextAttemptAt()), Types.TIMESTAMP_WITH_TIMEZONE);
        p.addValue("finishedAt", ts(job.finishedAt()), Types.TIMESTAMP_WITH_TIMEZONE);
        p.addValue("lastError", job.lastError(), Types.VARCHAR);
        p.addValue("result", job.result(), Types.VARCHAR);
        return p;
    }

    private static MapSqlParameterSource deliveryParams(Delivery d) {
        var p = new MapSqlParameterSource();
        p.addValue("jobId", d.jobId());
        p.addValue("attempt", d.attempt());
        p.addValue("leaseOwner", d.leaseOwner());
        p.addValue("leaseDeadline", ts(d.leaseDeadline()));
        p.addValue("ackState", d.ackState().name());
        p.addValue("queuedAt", ts(d.queuedAt()), Types.TIMESTAMP_WITH_TIMEZONE);
        p.addValue("startedAt", ts(d.startedAt()));
        p.addValue("heartbeatAt", ts(d.heartbeatAt()), Types.TIMESTAMP_WITH_TIMEZONE);
        p.addValue("finishedAt", ts(d.finishedAt()), Types.TIMESTAMP_WITH_TIMEZONE);
        p.addValue("progress", d.progress(), Types.INTEGER);
        p.addValue("error", d.error(), Types.VARCHAR);
        return p;
    }

    private static OffsetDateTime ts(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime t = rs.getObject(column, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }

    private static Instant instant(ResultSet rs, int column) throws SQLException {
        OffsetDateTime t = rs.getObject(column, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }

    private static final RowMapper<Job> JOB_MAPPER = (rs, i) -> new Job(
            rs.getObject("id", UUID.class),
            rs.getString("queue"),
            rs.getString("type"),
            rs.getString("payload"),
            JobState.valueOf(rs.getString("state")),
            rs.getLong("version"),
            rs.getInt("attempts"),
            rs.getInt("max_attempts"),
            Duration.ofMillis(rs.getLong("timeout_ms")),
            rs.getString("idempotency_key"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"),
            instant(rs, "ready_at"),
            instant(rs, "dispatched_at"),
            instant(rs, "next_attempt_at"),
            instant(rs, "finished_at"),
            rs.getString("last_error"),
            rs.getString("result"));

    private static final RowMapper<Delivery> DELIVERY_MAPPER = (rs, i) -> new Delivery(
            rs.getObject("job_id", UUID.class),
            rs.getInt("attempt"),
            rs.getString("lease_owner"),
            instant(rs, "lease_deadline"),
            AckState.valueOf(rs.getString("ack_state")),
            instant(rs, "queued_at"),
            instant(rs, "started_at"),
            instant(rs, "heartbeat_at"),
            instant(rs, "finished_at"),
            (Integer) rs.getObject("progress"),
            rs.getString("error"));
}
