package io.akasb.taskplatform.persistence;

import io.akasb.taskplatform.domain.Job;
import java.util.Optional;

public interface JobRepository {
    void save(Job job);
    Optional<Job> find(String id);
}
