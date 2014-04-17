package org.backmeup.job.impl;

import java.util.EventListener;

public interface JobReceivedListener extends EventListener {
	public void jobReceived(JobReceivedEvent jre);
}
