package io.akasb.taskplatform.dispatch;

public interface JobDispatcher {
    void dispatch(String jobId);
}
