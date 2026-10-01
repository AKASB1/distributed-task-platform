package io.akasb.taskplatform.persistence;

import io.akasb.taskplatform.domain.Job;
import java.util.Objects;

/** Result of {@link JobRepository#insert}: the stored job and whether this call created it. */
public record InsertResult(Job job, boolean created) {
    public InsertResult {
        Objects.requireNonNull(job, "job");
    }
}
