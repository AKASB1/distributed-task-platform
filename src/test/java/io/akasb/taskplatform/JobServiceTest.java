package io.akasb.taskplatform;

import io.akasb.taskplatform.api.JobService;
import io.akasb.taskplatform.dispatch.InMemoryDispatcher;
import io.akasb.taskplatform.domain.JobState;
import io.akasb.taskplatform.persistence.InMemoryJobRepository;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class JobServiceTest {
    @Test void submitsToLocalQueue() {
        InMemoryDispatcher dispatcher = new InMemoryDispatcher();
        JobService service = new JobService(new InMemoryJobRepository(), dispatcher);
        assertEquals(JobState.QUEUED, service.submit("one").state());
        assertEquals("one", dispatcher.poll());
        assertThrows(IllegalArgumentException.class, () -> service.submit("one"));
    }
}
