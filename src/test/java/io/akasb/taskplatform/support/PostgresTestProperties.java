package io.akasb.taskplatform.support;

import org.springframework.test.context.DynamicPropertyRegistry;

/** Points a Spring Boot test at a fresh embedded PostgreSQL database (Flyway runs on context start). */
public final class PostgresTestProperties {
    private PostgresTestProperties() {}

    public static void register(DynamicPropertyRegistry registry) {
        String url = EmbeddedPostgresSupport.newDatabase(false);
        registry.add("spring.datasource.url", () -> url);
        registry.add("spring.datasource.username", () -> EmbeddedPostgresSupport.USER);
        registry.add("spring.datasource.password", () -> "");
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "10");
    }
}
