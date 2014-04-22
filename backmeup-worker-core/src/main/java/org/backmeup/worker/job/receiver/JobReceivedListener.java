package org.backmeup.worker.job.receiver;

import java.util.EventListener;

public interface JobReceivedListener extends EventListener {
	public void jobReceived(JobReceivedEvent jre);
}
