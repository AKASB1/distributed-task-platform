package io.akasb.taskplatform.api;

import io.akasb.taskplatform.dispatch.JobDispatcher;
import io.akasb.taskplatform.domain.Job;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.persistence.JobRepository;

public final class JobService {
    private final JobRepository repository;
    private final JobDispatcher dispatcher;

    public JobService(JobRepository repository, JobDispatcher dispatcher) {
        this.repository = repository;
        this.dispatcher = dispatcher;
    }

    public Job submit(String id) {
        Job job = new Job(id);
        repository.save(job);
        job.transition(JobState.QUEUED, job.version());
        dispatcher.dispatch(id);
        return job;
    }

    public Job get(String id) {
        return repository.find(id).orElseThrow(() -> new IllegalArgumentException("unknown job"));
    }
}
