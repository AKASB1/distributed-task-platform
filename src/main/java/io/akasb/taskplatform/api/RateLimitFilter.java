package io.akasb.taskplatform.api;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.util.UrlPathHelper;

/**
 * Rate-limits job submission ({@code POST /v1/jobs}); every other request passes untouched.
 *
 * <p>The client is identified by the {@code X-Client-Id} header when it holds 1-64 visible ASCII characters, else by
 * the remote address. The two are kept in separate namespaces ({@code id:...} and {@code ip:...}), so a header value
 * cannot use up the budget of an address. The header is self-asserted: it separates cooperating clients, it does not
 * authenticate them.
 *
 * <p>A rejected request gets {@code 429 Too Many Requests} with a {@code Retry-After} header in whole seconds (rounded
 * up) and an RFC 7807 {@code application/problem+json} body. A limiter that throws lets the request through.
 */
public final class RateLimitFilter implements Filter {
    public static final String CLIENT_ID_HEADER = "X-Client-Id";
    public static final String PROBLEM_JSON = "application/problem+json";
    static final String LIMITED_PATH = "/v1/jobs";
    private static final Pattern CLIENT_ID = Pattern.compile("^[\\x21-\\x7E]{1,64}$");
    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

    private final RateLimiter limiter;
    private final Runnable onRejected;

    public RateLimitFilter(RateLimiter limiter) {
        this(limiter, () -> { });
    }

    /** @param onRejected called once per rejected request (for example to increment a counter) */
    public RateLimitFilter(RateLimiter limiter, Runnable onRejected) {
        this.limiter = Objects.requireNonNull(limiter);
        this.onRejected = Objects.requireNonNull(onRejected);
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
            throws IOException, ServletException {
        if (req instanceof HttpServletRequest request && res instanceof HttpServletResponse response
                && isSubmission(request)) {
            String client = clientKey(request);
            RateLimiter.Decision decision = decide(client);
            if (!decision.allowed()) {
                reject(response, decision.retryAfter());
                log.debug("job submission rejected by rate limit client={} retryAfterMs={}", client,
                        decision.retryAfter().toMillis());
                onRejected.run();
                return;
            }
        }
        chain.doFilter(req, res);
    }

    /** {@code POST /v1/jobs}, after removing the context path and {@code ;} parameters and decoding the path. */
    static boolean isSubmission(HttpServletRequest request) {
        return "POST".equals(request.getMethod())
                && LIMITED_PATH.equals(UrlPathHelper.defaultInstance.getPathWithinApplication(request));
    }

    static String clientKey(HttpServletRequest request) {
        String id = request.getHeader(CLIENT_ID_HEADER);
        if (id != null && CLIENT_ID.matcher(id).matches()) return "id:" + id;
        return "ip:" + request.getRemoteAddr();
    }

    /** Whole seconds, rounded up, at least 1 ({@code Retry-After: 0} would invite an immediate retry). */
    static long retryAfterSeconds(Duration retryAfter) {
        long millis = Math.max(0, retryAfter.toMillis());
        return Math.max(1, (millis + 999) / 1000);
    }

    private RateLimiter.Decision decide(String client) {
        try {
            return limiter.tryAcquire(client);
        } catch (RuntimeException e) {
            log.debug("rate limiter failed; request allowed", e);
            return RateLimiter.Decision.allow();
        }
    }

    private static void reject(HttpServletResponse response, Duration retryAfter) throws IOException {
        long seconds = retryAfterSeconds(retryAfter);
        String body = "{\"type\":\"about:blank\",\"title\":\"Too Many Requests\",\"status\":429,"
                + "\"detail\":\"Too many job submissions from this client; retry after " + seconds + " s.\"}";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        response.setStatus(429);
        response.setHeader("Retry-After", Long.toString(seconds));
        response.setContentType(PROBLEM_JSON);
        response.setContentLength(bytes.length);
        response.getOutputStream().write(bytes);
        response.flushBuffer();
    }
}
