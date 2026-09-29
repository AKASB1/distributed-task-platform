package io.akasb.taskplatform;

import io.akasb.taskplatform.api.JobService;
import io.akasb.taskplatform.dispatch.InMemoryDispatcher;
import io.akasb.taskplatform.persistence.InMemoryJobRepository;

/** Runs a local submission example. Network adapters are planned. */
public final class Application {
    private Application() {}

    public static void main(String[] args) {
        JobService service = new JobService(new InMemoryJobRepository(), new InMemoryDispatcher());
        System.out.println(service.submit("demo").state());
    }
}
