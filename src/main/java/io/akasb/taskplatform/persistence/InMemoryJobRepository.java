package io.akasb.taskplatform.persistence;

import io.akasb.taskplatform.domain.Job;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryJobRepository implements JobRepository {
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();

    public void save(Job job) {
        if (jobs.putIfAbsent(job.id(), job) != null) throw new IllegalArgumentException("duplicate job");
    }

    public Optional<Job> find(String id) { return Optional.ofNullable(jobs.get(id)); }
}
