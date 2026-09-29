package io.akasb.taskplatform.dispatch;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class InMemoryDispatcher implements JobDispatcher {
    private final Queue<String> queue = new ConcurrentLinkedQueue<>();
    public void dispatch(String jobId) { queue.add(jobId); }
    public String poll() { return queue.poll(); }
}
