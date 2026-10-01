package io.akasb.taskplatform.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.springframework.lang.NonNull;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Caps the size of every request body before it is parsed, so an oversized request is rejected with {@code 413}
 * instead of being read into memory first. A declared {@code Content-Length} above the limit is refused at once; a
 * chunked body is cut off when it crosses the limit. It applies to all paths on purpose: matching only
 * {@code /v1/jobs} could be bypassed with path variants that still route to the endpoint (for example
 * {@code /v1/jobs;x=1}), and no endpoint needs a larger body than a job submission.
 */
public final class RequestBodyLimitFilter extends OncePerRequestFilter {
    private final long maxBytes;

    public RequestBodyLimitFilter(long maxBytes) {
        if (maxBytes < 1) throw new IllegalArgumentException("maxBytes must be positive");
        this.maxBytes = maxBytes;
    }

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request, @NonNull HttpServletResponse response,
                                    @NonNull FilterChain chain) throws ServletException, IOException {
        if (request.getContentLengthLong() > maxBytes) {
            reject(response);
            return;
        }
        chain.doFilter(new LimitedRequest(request, maxBytes), response);
    }

    private void reject(HttpServletResponse response) throws IOException {
        response.setStatus(413);
        response.setContentType("application/problem+json");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.getWriter().write("{\"type\":\"about:blank\",\"title\":\"Payload too large\",\"status\":413,"
                + "\"detail\":\"request body exceeds " + maxBytes + " bytes\"}");
    }

    /** Thrown when a body without (or with a wrong) Content-Length crosses the limit; mapped to 413. */
    public static final class BodyTooLargeException extends IOException {
        BodyTooLargeException(long maxBytes) {
            super("request body exceeds " + maxBytes + " bytes");
        }
    }

    private static final class LimitedRequest extends HttpServletRequestWrapper {
        private final long maxBytes;
        private ServletInputStream stream;

        LimitedRequest(HttpServletRequest request, long maxBytes) {
            super(request);
            this.maxBytes = maxBytes;
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            if (stream == null) stream = new LimitedStream(super.getInputStream(), maxBytes);
            return stream;
        }
    }

    private static final class LimitedStream extends ServletInputStream {
        private final ServletInputStream delegate;
        private final long maxBytes;
        private long read;

        LimitedStream(ServletInputStream delegate, long maxBytes) {
            this.delegate = delegate;
            this.maxBytes = maxBytes;
        }

        @Override
        public int read() throws IOException {
            int b = delegate.read();
            if (b >= 0) count(1);
            return b;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int n = delegate.read(buffer, offset, length);
            if (n > 0) count(n);
            return n;
        }

        private void count(int n) throws IOException {
            read += n;
            if (read > maxBytes) throw new BodyTooLargeException(maxBytes);
        }

        @Override
        public boolean isFinished() {
            return delegate.isFinished();
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public void setReadListener(ReadListener listener) {
            delegate.setReadListener(listener);
        }

        @Override
        public int available() throws IOException {
            return delegate.available();
        }
    }
}
