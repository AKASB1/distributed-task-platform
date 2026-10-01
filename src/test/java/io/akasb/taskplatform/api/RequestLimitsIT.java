package io.akasb.taskplatform.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.support.PostgresTestProperties;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Size and content limits of {@code POST /v1/jobs} over real HTTP. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RequestLimitsIT {
    private final HttpClient http = HttpClient.newBuilder().proxy(HttpClient.Builder.NO_PROXY).build();
    private final ObjectMapper mapper = new ObjectMapper();

    @LocalServerPort
    int port;

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        PostgresTestProperties.register(registry);
    }

    private String bigBody(int payloadChars) {
        return "{\"type\":\"sleep\",\"payload\":{\"pad\":\"" + "x".repeat(payloadChars) + "\"}}";
    }

    @Test
    void bodyAboveTheLimitWithContentLengthIs413() throws Exception {
        HttpResponse<String> r = post(HttpRequest.BodyPublishers.ofString(bigBody(200_000)));
        assertThat(r.statusCode()).isEqualTo(413);
        assertThat(r.headers().firstValue("Content-Type")).hasValueSatisfying(
                v -> assertThat(v).startsWith("application/problem+json"));
        assertThat(mapper.readTree(r.body()).path("status").asInt()).isEqualTo(413);
    }

    @Test
    void pathVariantsThatRouteToTheEndpointAreLimitedToo() throws Exception {
        for (String path : new String[] {"/v1/jobs;x=1", "/v1//jobs", "/v1/%6Aobs"}) {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofInputStream(
                            () -> new ByteArrayInputStream(bigBody(200_000).getBytes(StandardCharsets.UTF_8))))
                    .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(r.statusCode()).as(path).isIn(400, 404, 413);
            assertThat(r.statusCode()).as(path + " must never be accepted").isNotIn(200, 201);
        }
    }

    @Test
    void chunkedBodyAboveTheLimitIs413() throws Exception {
        byte[] body = bigBody(200_000).getBytes(StandardCharsets.UTF_8);
        // ofInputStream has no known length, so the client sends it chunked (no Content-Length header)
        HttpResponse<String> r = post(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(body)));
        assertThat(r.statusCode()).isEqualTo(413);
        assertThat(mapper.readTree(r.body()).path("title").asText()).isEqualTo("Payload too large");
    }

    @Test
    void payloadJustAboveThePayloadLimitButWithinTheBodyLimitIsAValidationError() throws Exception {
        HttpResponse<String> r = post(HttpRequest.BodyPublishers.ofString(bigBody(66_000)));
        assertThat(r.statusCode()).isEqualTo(400);
        assertThat(mapper.readTree(r.body()).path("detail").asText()).contains("exceeds");
    }

    @Test
    void nulCharacterInThePayloadIs400Not500() throws Exception {
        HttpResponse<String> r = post(HttpRequest.BodyPublishers.ofString(
                "{\"type\":\"sleep\",\"payload\":{\"v\":\"a\\u0000b\"}}"));
        assertThat(r.statusCode()).isEqualTo(400);
        assertThat(mapper.readTree(r.body()).path("detail").asText()).contains("NUL");
    }

    @Test
    void malformedJsonIsStill400() throws Exception {
        assertThat(post(HttpRequest.BodyPublishers.ofString("{not json")).statusCode()).isEqualTo(400);
    }

    private HttpResponse<String> post(HttpRequest.BodyPublisher body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/jobs"))
                .header("Content-Type", "application/json").POST(body).build(), HttpResponse.BodyHandlers.ofString());
    }
}
