package io.akasb.taskplatform.persistence;

/** Runs the {@link JobRepositoryContract} against the in-memory store that backs the fast unit tests. */
class InMemoryJobRepositoryTest extends JobRepositoryContract {

    @Override
    protected JobRepository newRepository() {
        return new InMemoryJobRepository();
    }
}
