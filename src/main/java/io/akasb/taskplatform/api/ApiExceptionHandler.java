package io.akasb.taskplatform.api;

import io.akasb.taskplatform.dispatch.JobNotFoundException;
import io.akasb.taskplatform.dispatch.JobStateConflictException;
import java.util.List;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** Maps errors to RFC 7807 problem responses. */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(InvalidJobRequestException.class)
    public ProblemDetail invalid(InvalidJobRequestException e) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid job request", e.getMessage());
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail typeMismatch(MethodArgumentTypeMismatchException e) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid parameter",
                "'" + e.getName() + "' has an invalid value: " + e.getValue());
    }

    @ExceptionHandler(JobNotFoundException.class)
    public ProblemDetail notFound(JobNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, "Job not found", e.getMessage());
    }

    @ExceptionHandler(JobStateConflictException.class)
    public ProblemDetail conflict(JobStateConflictException e) {
        ProblemDetail p = problem(HttpStatus.CONFLICT, "Job state conflict", e.getMessage());
        if (e.job() != null) p.setProperty("state", e.job().state().name());
        return p;
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ProblemDetail idempotency(IdempotencyConflictException e) {
        ProblemDetail p = problem(HttpStatus.UNPROCESSABLE_ENTITY, "Idempotency key reused", e.getMessage());
        p.setProperty("existingJobId", e.existingJobId().toString());
        return p;
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(@NonNull HttpMessageNotReadableException ex,
                                                                  @NonNull HttpHeaders headers,
                                                                  @NonNull HttpStatusCode status,
                                                                  @NonNull WebRequest request) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof RequestBodyLimitFilter.BodyTooLargeException tooLarge) {
                return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE)
                        .body(problem(HttpStatus.PAYLOAD_TOO_LARGE, "Payload too large", tooLarge.getMessage()));
            }
        }
        return ResponseEntity.badRequest().body(problem(HttpStatus.BAD_REQUEST, "Malformed request body",
                "the request body is not valid JSON for this endpoint"));
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(@NonNull MethodArgumentNotValidException ex,
                                                                  @NonNull HttpHeaders headers,
                                                                  @NonNull HttpStatusCode status,
                                                                  @NonNull WebRequest request) {
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + " " + f.getDefaultMessage())
                .sorted()
                .toList();
        ProblemDetail p = problem(HttpStatus.BAD_REQUEST, "Invalid job request", String.join("; ", errors));
        p.setProperty("errors", errors);
        return ResponseEntity.badRequest().body(p);
    }

    private static ProblemDetail problem(HttpStatus status, String title, String detail) {
        ProblemDetail p = ProblemDetail.forStatusAndDetail(status, detail);
        p.setTitle(title);
        return p;
    }
}
