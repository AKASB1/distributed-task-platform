package io.akasb.taskplatform.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/** The servlet filter in front of {@code POST /v1/jobs}, with plain servlet mocks. */
class RateLimitFilterTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final RecordingLimiter limiter = new RecordingLimiter();
    private final AtomicInteger rejections = new AtomicInteger();
    private final RateLimitFilter filter = new RateLimitFilter(limiter, rejections::incrementAndGet);

    private static MockHttpServletRequest post(String uri) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", uri);
        request.setRemoteAddr("10.1.2.3");
        return request;
    }

    private MockHttpServletResponse run(MockHttpServletRequest request, MockFilterChain chain) throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, chain);
        return response;
    }

    @Test
    void rejectedSubmissionGets429WithRetryAfterAndProblemJson() throws Exception {
        limiter.next = RateLimiter.Decision.reject(Duration.ofMillis(1200));
        MockFilterChain chain = new MockFilterChain();

        MockHttpServletResponse response = run(post("/v1/jobs"), chain);

        assertThat(response.getStatus()).isEqualTo(429);
        assertThat(response.getHeader("Retry-After")).isEqualTo("2");
        assertThat(response.getContentType()).startsWith("application/problem+json");
        JsonNode body = mapper.readTree(response.getContentAsByteArray());
        assertThat(body.path("type").asText()).isEqualTo("about:blank");
        assertThat(body.path("title").asText()).isEqualTo("Too Many Requests");
        assertThat(body.path("status").asInt()).isEqualTo(429);
        assertThat(body.path("detail").asText()).contains("retry after 2 s");
        assertThat(chain.getRequest()).as("the controller is never reached").isNull();
        assertThat(rejections).hasValue(1);
    }

    @Test
    void allowedSubmissionContinuesDownTheChain() throws Exception {
        MockFilterChain chain = new MockFilterChain();

        MockHttpServletResponse response = run(post("/v1/jobs"), chain);

        assertThat(chain.getRequest()).isNotNull();
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getHeader("Retry-After")).isNull();
        assertThat(limiter.keys).containsExactly("ip:10.1.2.3");
        assertThat(rejections).hasValue(0);
    }

    @ParameterizedTest
    @CsvSource({"GET,/v1/jobs", "GET,/v1/jobs/0b0e5d55-1a8e-4bb4-8a4c-46c1d1fd3a3e",
            "POST,/v1/jobs/0b0e5d55-1a8e-4bb4-8a4c-46c1d1fd3a3e/cancel", "GET,/v1/queues/default/stats",
            "GET,/actuator/health", "POST,/v1/jobsx", "POST,/v1", "PUT,/v1/jobs"})
    void otherRequestsAreUntouched(String method, String uri) throws Exception {
        limiter.next = RateLimiter.Decision.reject(Duration.ofSeconds(1));
        MockHttpServletRequest request = new MockHttpServletRequest(method, uri);
        MockFilterChain chain = new MockFilterChain();

        MockHttpServletResponse response = run(request, chain);

        assertThat(chain.getRequest()).isSameAs(request);
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(limiter.keys).as("limiter not consulted").isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"/v1/jobs;jsessionid=abc", "//v1//jobs", "/v1/job%73"})
    void spellingsThatRouteToTheSubmitEndpointAreLimitedToo(String uri) throws Exception {
        limiter.next = RateLimiter.Decision.reject(Duration.ofSeconds(1));
        assertThat(run(post(uri), new MockFilterChain()).getStatus()).isEqualTo(429);
    }

    @Test
    void contextPathIsIgnored() throws Exception {
        limiter.next = RateLimiter.Decision.reject(Duration.ofSeconds(1));
        MockHttpServletRequest request = post("/api/v1/jobs");
        request.setContextPath("/api");
        assertThat(run(request, new MockFilterChain()).getStatus()).isEqualTo(429);
    }

    @Test
    void clientIdHeaderIdentifiesTheClient() throws Exception {
        MockHttpServletRequest request = post("/v1/jobs");
        request.addHeader(RateLimitFilter.CLIENT_ID_HEADER, "tenant-42:batch");
        run(request, new MockFilterChain());
        assertThat(limiter.keys).containsExactly("id:tenant-42:batch");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "has space", "tab\there", "café"})
    void invalidClientIdFallsBackToTheRemoteAddress(String header) throws Exception {
        MockHttpServletRequest request = post("/v1/jobs");
        request.addHeader(RateLimitFilter.CLIENT_ID_HEADER, header);
        run(request, new MockFilterChain());
        assertThat(limiter.keys).containsExactly("ip:10.1.2.3");
    }

    @Test
    void clientIdLongerThan64CharactersFallsBackToTheRemoteAddress() throws Exception {
        MockHttpServletRequest ok = post("/v1/jobs");
        ok.addHeader(RateLimitFilter.CLIENT_ID_HEADER, "x".repeat(64));
        MockHttpServletRequest tooLong = post("/v1/jobs");
        tooLong.addHeader(RateLimitFilter.CLIENT_ID_HEADER, "x".repeat(65));

        run(ok, new MockFilterChain());
        run(tooLong, new MockFilterChain());

        assertThat(limiter.keys).containsExactly("id:" + "x".repeat(64), "ip:10.1.2.3");
    }

    @Test
    void headerCannotImpersonateAnAddress() throws Exception {
        MockHttpServletRequest request = post("/v1/jobs");
        request.addHeader(RateLimitFilter.CLIENT_ID_HEADER, "10.1.2.3");
        run(request, new MockFilterChain());
        assertThat(limiter.keys).containsExactly("id:10.1.2.3");
    }

    @Test
    void failingLimiterLetsTheRequestThrough() throws Exception {
        RateLimitFilter failOpen = new RateLimitFilter(client -> {
            throw new IllegalStateException("store down");
        });
        MockFilterChain chain = new MockFilterChain();
        MockHttpServletResponse response = new MockHttpServletResponse();

        failOpen.doFilter(post("/v1/jobs"), response, chain);

        assertThat(chain.getRequest()).isNotNull();
        assertThat(response.getStatus()).isEqualTo(200);
    }

    @ParameterizedTest
    @CsvSource({"0,1", "1,1", "999,1", "1000,1", "1001,2", "1999,2", "2000,2", "61500,62"})
    void retryAfterIsRoundedUpToWholeSecondsAndAtLeastOne(long millis, long seconds) {
        assertThat(RateLimitFilter.retryAfterSeconds(Duration.ofMillis(millis))).isEqualTo(seconds);
    }

    /** Records client keys and answers with {@link #next}. */
    static final class RecordingLimiter implements RateLimiter {
        final List<String> keys = new CopyOnWriteArrayList<>();
        volatile Decision next = Decision.allow();

        @Override
        public Decision tryAcquire(String clientKey) {
            keys.add(clientKey);
            return next;
        }
    }
}
