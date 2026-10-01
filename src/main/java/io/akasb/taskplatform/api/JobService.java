package io.akasb.taskplatform.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.akasb.taskplatform.dispatch.JobLifecycle;
import io.akasb.taskplatform.dispatch.JobNotFoundException;
import io.akasb.taskplatform.dispatch.SubmitCommand;
import io.akasb.taskplatform.dispatch.SubmitResult;
import io.akasb.taskplatform.domain.Delivery;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.persistence.JobRepository;
import io.akasb.taskplatform.persistence.QueueStats;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application service behind the REST API: validates requests, applies defaults, enforces idempotency semantics and
 * delegates state changes to the {@link JobLifecycle}. Framework-free so it can be unit-tested directly.
 */
public final class JobService {
    public static final String QUEUE_NAME_PATTERN = "^[a-z0-9][a-z0-9_-]{0,63}$";
    public static final String DEFAULT_QUEUE = "default";
    private static final Logger log = LoggerFactory.getLogger(JobService.class);
    private static final java.util.regex.Pattern QUEUE_NAME = java.util.regex.Pattern.compile(QUEUE_NAME_PATTERN);
    private static final java.util.regex.Pattern IDEMPOTENCY_KEY = java.util.regex.Pattern.compile("^[\\x21-\\x7E]{1,255}$");

    /** Request limits and defaults. {@code servedQueues} empty means any queue name is accepted. */
    public record Limits(int defaultMaxAttempts, int maxAttemptsLimit, Duration defaultTimeout, Duration maxTimeout,
                         int maxPayloadBytes) {
    }

    public record SubmitOutcome(Job job, boolean created) { }

    public record JobDetails(Job job, List<Delivery> deliveries) { }

    private final JobLifecycle lifecycle;
    private final JobRepository repository;
    private final Supplier<Set<String>> knownTypes;
    private final Supplier<Set<String>> servedQueues;
    private final Limits limits;
    private final ObjectMapper mapper;
    private final IdempotencyCache idempotencyCache;

    /** Without an idempotency cache: every keyed submission goes to the database. */
    public JobService(JobLifecycle lifecycle, JobRepository repository, Supplier<Set<String>> knownTypes,
                      Supplier<Set<String>> servedQueues, Limits limits, ObjectMapper mapper) {
        this(lifecycle, repository, knownTypes, servedQueues, limits, mapper, IdempotencyCache.none());
    }

    public JobService(JobLifecycle lifecycle, JobRepository repository, Supplier<Set<String>> knownTypes,
                      Supplier<Set<String>> servedQueues, Limits limits, ObjectMapper mapper,
                      IdempotencyCache idempotencyCache) {
        this.lifecycle = Objects.requireNonNull(lifecycle);
        this.repository = Objects.requireNonNull(repository);
        this.knownTypes = Objects.requireNonNull(knownTypes);
        this.servedQueues = Objects.requireNonNull(servedQueues);
        this.limits = Objects.requireNonNull(limits);
        this.mapper = Objects.requireNonNull(mapper);
        this.idempotencyCache = Objects.requireNonNull(idempotencyCache);
    }

    /**
     * Submits a job. With an idempotency key, a repeated identical request returns the original job
     * ({@code created = false}); a different request with the same key is rejected.
     *
     * <p>With a key, the idempotency cache is asked first. Its answer counts only if the database holds that job
     * under the same key; then the request is answered as a replay without an insert. In every other case (miss,
     * stale entry, cache error) the database path runs and stays authoritative, and its answer is remembered.
     */
    public SubmitOutcome submit(SubmitJobRequest request, String idempotencyKey) {
        SubmitCommand command = validate(request, idempotencyKey);
        String key = command.idempotencyKey();
        if (key != null) {
            Optional<Job> cached = cachedJob(key);
            if (cached.isPresent()) return replay(cached.get(), command, request);
        }
        SubmitResult result = lifecycle.submit(command);
        if (key != null) remember(key, result.job().id());
        if (!result.created()) return replay(result.job(), command, request);
        return new SubmitOutcome(result.job(), true);
    }

    private SubmitOutcome replay(Job existing, SubmitCommand command, SubmitJobRequest request) {
        if (!sameRequest(existing, command, request)) {
            throw new IdempotencyConflictException(command.idempotencyKey(), existing.id());
        }
        return new SubmitOutcome(existing, false);
    }

    /** The job the cache names for {@code key}, if the database confirms it carries that key. */
    private Optional<Job> cachedJob(String key) {
        Optional<UUID> hint;
        try {
            hint = idempotencyCache.find(key);
        } catch (RuntimeException e) {
            log.debug("idempotency cache lookup failed; using the database", e);
            return Optional.empty();
        }
        if (hint == null || hint.isEmpty()) return Optional.empty();
        Optional<Job> job = repository.find(hint.get()).filter(j -> key.equals(j.idempotencyKey()));
        if (job.isEmpty()) log.debug("stale idempotency cache entry ignored for job {}", hint.get());
        return job;
    }

    private void remember(String key, UUID jobId) {
        try {
            idempotencyCache.remember(key, jobId);
        } catch (RuntimeException e) {
            log.debug("idempotency cache update failed", e);
        }
    }

    public JobDetails get(UUID id) {
        Job job = repository.find(id).orElseThrow(() -> new JobNotFoundException(id));
        return new JobDetails(job, repository.deliveries(id));
    }

    public JobDetails cancel(UUID id) {
        Job job = lifecycle.cancel(id);
        return new JobDetails(job, repository.deliveries(id));
    }

    public QueueStats queueStats(String queue) {
        if (queue == null || !QUEUE_NAME.matcher(queue).matches()) {
            throw new InvalidJobRequestException("queue name must match " + QUEUE_NAME_PATTERN);
        }
        return repository.queueStats(queue, lifecycle.now());
    }

    SubmitCommand validate(SubmitJobRequest request, String idempotencyKey) {
        if (request == null) throw new InvalidJobRequestException("request body required");
        if (request.type() == null || request.type().isBlank()) throw new InvalidJobRequestException("type is required");
        if (!knownTypes.get().contains(request.type())) {
            throw new InvalidJobRequestException("unknown job type '" + request.type() + "'; known types: "
                    + knownTypes.get().stream().sorted().toList());
        }
        String queue = request.queue() == null ? DEFAULT_QUEUE : request.queue();
        if (!QUEUE_NAME.matcher(queue).matches()) {
            throw new InvalidJobRequestException("queue name must match " + QUEUE_NAME_PATTERN);
        }
        Set<String> served = servedQueues.get();
        if (!served.isEmpty() && !served.contains(queue)) {
            throw new InvalidJobRequestException("no worker pool serves queue '" + queue + "'; served queues: "
                    + served.stream().sorted().toList());
        }
        int maxAttempts = request.maxAttempts() == null ? limits.defaultMaxAttempts() : request.maxAttempts();
        if (maxAttempts < 1 || maxAttempts > limits.maxAttemptsLimit()) {
            throw new InvalidJobRequestException("maxAttempts must be between 1 and " + limits.maxAttemptsLimit());
        }
        Duration timeout = request.timeoutMs() == null ? limits.defaultTimeout() : Duration.ofMillis(request.timeoutMs());
        if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(limits.maxTimeout()) > 0) {
            throw new InvalidJobRequestException("timeoutMs must be between 1 and " + limits.maxTimeout().toMillis());
        }
        JsonNode payload = request.payload() == null || request.payload().isNull()
                ? mapper.createObjectNode() : request.payload();
        if (!payload.isObject()) throw new InvalidJobRequestException("payload must be a JSON object");
        if (!storable(payload)) {
            throw new InvalidJobRequestException(
                    "payload must not contain NUL characters (\\u0000) or unpaired UTF-16 surrogates");
        }
        String payloadJson;
        try {
            payloadJson = mapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new InvalidJobRequestException("payload is not serialisable");
        }
        if (payloadJson.getBytes(StandardCharsets.UTF_8).length > limits.maxPayloadBytes()) {
            throw new InvalidJobRequestException("payload exceeds " + limits.maxPayloadBytes() + " bytes");
        }
        if (idempotencyKey != null && !IDEMPOTENCY_KEY.matcher(idempotencyKey).matches()) {
            throw new InvalidJobRequestException("Idempotency-Key must be 1-255 visible ASCII characters");
        }
        return new SubmitCommand(queue, request.type(), payloadJson, maxAttempts, timeout, idempotencyKey);
    }

    /**
     * Whether a replay asks for the same job. Fields the request left out are not compared, so a replay sent after a
     * server default changed (for example the default attempt count) is still recognised as the same request.
     */
    private boolean sameRequest(Job existing, SubmitCommand command, SubmitJobRequest request) {
        return existing.queue().equals(command.queue())
                && existing.type().equals(command.type())
                && (request.maxAttempts() == null || existing.maxAttempts() == command.maxAttempts())
                && (request.timeoutMs() == null || existing.timeout().equals(command.timeout()))
                && sameJson(existing.payload(), command.payload());
    }

    /**
     * Semantic JSON equality: key order is ignored (the store may reorder keys) and numbers compare by value (the
     * store may rewrite {@code 1.2345678E7} as {@code 12345678}).
     */
    /** PostgreSQL jsonb rejects U+0000 and cannot represent an unpaired surrogate (it would be stored as '?'). */
    private static boolean storable(JsonNode node) {
        if (node.isTextual()) return storable(node.textValue());
        if (node.isObject()) {
            for (var it = node.properties().iterator(); it.hasNext(); ) {
                var e = it.next();
                if (!storable(e.getKey()) || !storable(e.getValue())) return false;
            }
            return true;
        }
        if (node.isArray()) {
            for (JsonNode child : node) {
                if (!storable(child)) return false;
            }
        }
        return true;
    }

    private static boolean storable(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\u0000') return false;
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= text.length() || !Character.isLowSurrogate(text.charAt(i + 1))) return false;
                i++;
            } else if (Character.isLowSurrogate(c)) {
                return false;
            }
        }
        return true;
    }

    private boolean sameJson(String a, String b) {
        try {
            return mapper.readTree(a).equals(NUMERIC_VALUE_ORDER, mapper.readTree(b));
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    private static final java.util.Comparator<JsonNode> NUMERIC_VALUE_ORDER = (x, y) -> {
        if (x.equals(y)) return 0;
        if (x.isNumber() && y.isNumber()) return x.decimalValue().compareTo(y.decimalValue()) == 0 ? 0 : 1;
        return 1;
    };
}
