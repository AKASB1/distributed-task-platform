package io.akasb.taskplatform.support;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import org.flywaydb.core.Flyway;

/**
 * One real PostgreSQL server per test JVM (zonky embedded binaries, no Docker), started lazily. Every caller gets a
 * fresh database, so test classes are isolated without restarting the server. Files live under {@code target/}.
 */
public final class EmbeddedPostgresSupport {
    public static final String USER = "postgres";
    private static final AtomicInteger DATABASES = new AtomicInteger();
    private static EmbeddedPostgres postgres;

    private EmbeddedPostgresSupport() {}

    public static synchronized EmbeddedPostgres server() {
        if (postgres == null) {
            Path base = Path.of("target", "embedded-pg").toAbsolutePath();
            try {
                postgres = EmbeddedPostgres.builder()
                        .setOverrideWorkingDirectory(base.resolve("work").toFile())
                        .setDataDirectory(new File(base.toFile(), "data-" + ProcessHandle.current().pid()))
                        .setCleanDataDirectory(true)
                        .setServerConfig("max_connections", "400")
                        .setServerConfig("fsync", "off")
                        .setServerConfig("synchronous_commit", "off")
                        .start();
            } catch (IOException e) {
                throw new UncheckedIOException("could not start embedded PostgreSQL", e);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(EmbeddedPostgresSupport::shutdown, "embedded-pg-stop"));
        }
        return postgres;
    }

    /** Creates an empty database and returns its JDBC URL. With {@code migrate}, the Flyway schema is applied. */
    public static String newDatabase(boolean migrate) {
        String name = "tp_" + ProcessHandle.current().pid() + "_" + DATABASES.incrementAndGet();
        try (Connection c = server().getPostgresDatabase().getConnection(); Statement s = c.createStatement()) {
            s.execute("CREATE DATABASE " + name);
        } catch (SQLException e) {
            throw new IllegalStateException("could not create database " + name, e);
        }
        String url = "jdbc:postgresql://localhost:" + server().getPort() + "/" + name;
        if (migrate) {
            Flyway.configure().dataSource(url, USER, "").locations("classpath:db/migration").load().migrate();
        }
        return url;
    }

    /** A pooled DataSource on a fresh, migrated database. Close it at the end of the test class. */
    public static HikariDataSource newMigratedDataSource(int poolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(newDatabase(true));
        config.setUsername(USER);
        config.setPassword("");
        config.setMaximumPoolSize(poolSize);
        config.setConnectionTimeout(10_000);
        return new HikariDataSource(config);
    }

    private static synchronized void shutdown() {
        if (postgres != null) {
            try {
                postgres.close();
            } catch (IOException ignored) {
                // JVM is exiting
            }
            postgres = null;
        }
    }
}
