package org.backmeup.worker.job.receiver;

import java.util.EventListener;

public interface JobReceivedListener extends EventListener {
    void jobReceived(JobReceivedEvent jre);
}
