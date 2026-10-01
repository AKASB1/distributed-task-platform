package io.akasb.taskplatform;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Local run without Docker or an installed PostgreSQL: starts an embedded PostgreSQL 17 (binaries from Maven, data in
 * {@code tmp/dev-postgres}, kept across restarts) and then the service against it.
 *
 * <pre>./mvnw spring-boot:test-run</pre>
 *
 * Stop it with Ctrl+C or a JSON POST to {@code http://127.0.0.1:18080/actuator/shutdown}.
 *
 * System properties: {@code taskplatform.dev.pg-dir} (default {@code tmp/dev-postgres}),
 * {@code taskplatform.dev.pg-clean=true} to start from an empty database. Lives on the test classpath so the
 * production jar does not ship native PostgreSQL binaries.
 */
public final class LocalDevApplication {
    private static final String DATABASE = "taskplatform";

    private LocalDevApplication() {}

    public static void main(String[] args) throws IOException, SQLException {
        Path base = Path.of(System.getProperty("taskplatform.dev.pg-dir", "tmp/dev-postgres")).toAbsolutePath();
        EmbeddedPostgres postgres = EmbeddedPostgres.builder()
                .setOverrideWorkingDirectory(base.resolve("work").toFile())
                .setDataDirectory(base.resolve("data").toFile())
                .setCleanDataDirectory(Boolean.getBoolean("taskplatform.dev.pg-clean"))
                .start();
        try (Connection c = postgres.getPostgresDatabase().getConnection(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1 FROM pg_database WHERE datname = '" + DATABASE + "'")) {
            if (!rs.next()) s.execute("CREATE DATABASE " + DATABASE);
        }
        String url = "jdbc:postgresql://localhost:" + postgres.getPort() + "/" + DATABASE;
        System.setProperty("spring.datasource.url", url);
        System.setProperty("spring.datasource.username", "postgres");
        System.setProperty("spring.datasource.password", "");
        // Local runs only (bound to 127.0.0.1): allow "POST /actuator/shutdown" for a clean stop that also stops
        // the embedded PostgreSQL through its shutdown hook.
        System.setProperty("management.endpoint.shutdown.access", "unrestricted");
        System.setProperty("management.endpoints.web.exposure.include", "health,info,prometheus,metrics,shutdown");
        System.out.println("embedded PostgreSQL ready at " + url + " (data: " + base.resolve("data") + ")");
        Application.main(args);
    }
}
