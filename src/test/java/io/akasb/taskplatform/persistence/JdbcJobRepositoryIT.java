package io.akasb.taskplatform.persistence;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.support.EmbeddedPostgresSupport;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Runs the {@link JobRepositoryContract} against PostgreSQL 17 (embedded binaries, schema applied by Flyway) and adds
 * what only the real database can show: concurrent idempotent inserts, concurrent compare-and-set races and the
 * schema constraints. The database is fresh for this class and emptied before every test.
 */
class JdbcJobRepositoryIT extends JobRepositoryContract {
    private static final int THREADS = 8;
    private static final int ROUNDS = 20;
    private static HikariDataSource dataSource;

    @BeforeAll
    static void createDatabase() {
        dataSource = EmbeddedPostgresSupport.newMigratedDataSource(16);
    }

    @AfterAll
    static void closeDatabase() {
        if (dataSource != null) dataSource.close();
    }

    @Override
    protected JobRepository newRepository() {
        execute("TRUNCATE deliveries, jobs");
        return new JdbcJobRepository(dataSource);
    }

    // ---------------------------------------------------------------- concurrency

    @Test
    void concurrentInsertsWithTheSameIdempotencyKeyCreateExactlyOneJob() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            for (int round = 0; round < ROUNDS; round++) {
                String key = "race-" + round;
                List<Job> candidates = new ArrayList<>();
                for (int i = 0; i < THREADS; i++) {
                    candidates.add(Job.pending(newId(), QUEUE, "sleep", "{\"i\":" + i + "}", 3,
                            Duration.ofSeconds(30), key, t(i)));
                }

                List<InsertResult> results = race(pool, candidates.stream()
                        .<Callable<InsertResult>>map(job -> () -> repository.insert(job))
                        .toList());

                assertThat(results).as("round %d", round).filteredOn(InsertResult::created).hasSize(1);
                UUID winnerId = results.stream().filter(InsertResult::created).findFirst().orElseThrow().job().id();
                Job winner = candidates.stream().filter(j -> j.id().equals(winnerId)).findFirst().orElseThrow();
                for (InsertResult result : results) {
                    assertSameJob(result.job(), winner);
                }
                assertThat(count("SELECT count(*) FROM jobs WHERE idempotency_key = '" + key + "'")).isEqualTo(1);
                assertSameJob(stored(winnerId), winner);
            }
            assertThat(count("SELECT count(*) FROM jobs")).isEqualTo(ROUNDS);
        } finally {
            shutdown(pool);
        }
    }

    @Test
    void concurrentUpdatesOfTheSameVersionHaveExactlyOneWinner() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            for (int round = 0; round < ROUNDS; round++) {
                Job queued = insertQueued(QUEUE, t(round));
                List<Job> contenders = new ArrayList<>();
                for (int i = 0; i < THREADS; i++) contenders.add(queued.start(t(100 + i)));

                List<Boolean> outcomes = race(pool, contenders.stream()
                        .<Callable<Boolean>>map(next -> () -> repository.update(next, queued.version()))
                        .toList());

                assertThat(outcomes).as("round %d", round).filteredOn(Boolean::booleanValue).hasSize(1);
                Job winner = contenders.get(outcomes.indexOf(Boolean.TRUE));
                assertSameJob(stored(queued.id()), winner);
            }
        } finally {
            shutdown(pool);
        }
    }

    // ---------------------------------------------------------------- schema

    @Test
    void schemaRejectsAnUnknownJobState() throws SQLException {
        String insert = "INSERT INTO jobs (id, queue, type, payload, state, version, attempts, max_attempts,"
                + " timeout_ms, created_at, updated_at) VALUES (?, 'default', 'sleep', '{}'::jsonb, ?, 0, 0, 1, 1000,"
                + " now(), now())";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(insert)) {
            ps.setObject(1, newId());
            ps.setString(2, "PENDING");
            assertThat(ps.executeUpdate()).as("control insert").isEqualTo(1);

            ps.setObject(1, newId());
            ps.setString(2, "BOGUS");
            assertThatThrownBy(ps::executeUpdate)
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("jobs_state_check")
                    .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("23514"));
        }
        assertThat(count("SELECT count(*) FROM jobs")).isEqualTo(1);
    }

    @Test
    void schemaRejectsProgressOutsideZeroToHundred() throws SQLException {
        Job job = insertPending(QUEUE, t(0));
        String insert = "INSERT INTO deliveries (job_id, attempt, lease_owner, lease_deadline, ack_state, started_at,"
                + " progress) VALUES (?, ?, 'worker-a', now(), 'LEASED', now(), ?)";
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(insert)) {
            ps.setObject(1, job.id());
            ps.setInt(2, 1);
            ps.setInt(3, 100);
            assertThat(ps.executeUpdate()).as("control insert").isEqualTo(1);

            ps.setInt(2, 2);
            ps.setInt(3, 150);
            assertThatThrownBy(ps::executeUpdate)
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("deliveries_progress_check")
                    .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("23514"));

            ps.setInt(2, 3);
            ps.setInt(3, -1);
            assertThatThrownBy(ps::executeUpdate)
                    .isInstanceOf(SQLException.class)
                    .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("23514"));
        }
        assertThat(repository.deliveries(job.id())).hasSize(1);
    }

    @Test
    void flywayHistoryRecordsVersionOne() throws SQLException {
        Map<String, Boolean> versions = new LinkedHashMap<>();
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT version, success FROM flyway_schema_history"
                     + " WHERE version IS NOT NULL ORDER BY installed_rank")) {
            while (rs.next()) versions.put(rs.getString(1), rs.getBoolean(2));
        }
        assertThat(versions).containsEntry("1", true);
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Starts one task per thread, waits until every thread is parked on the start latch, releases them together and
     * returns the results in task order.
     */
    private static <T> List<T> race(ExecutorService pool, List<Callable<T>> tasks) throws Exception {
        CountDownLatch ready = new CountDownLatch(tasks.size());
        CountDownLatch go = new CountDownLatch(1);
        List<Future<T>> futures = new ArrayList<>();
        for (Callable<T> task : tasks) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                if (!go.await(20, SECONDS)) throw new IllegalStateException("start signal never came");
                return task.call();
            }));
        }
        assertThat(ready.await(20, SECONDS)).as("all threads ready").isTrue();
        go.countDown();
        List<T> results = new ArrayList<>();
        for (Future<T> future : futures) results.add(future.get(20, SECONDS));
        return results;
    }

    private static void shutdown(ExecutorService pool) throws InterruptedException {
        pool.shutdownNow();
        assertThat(pool.awaitTermination(20, SECONDS)).as("pool terminated").isTrue();
    }

    private static void execute(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(sql, e);
        }
    }

    private static long count(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(sql, e);
        }
    }
}
