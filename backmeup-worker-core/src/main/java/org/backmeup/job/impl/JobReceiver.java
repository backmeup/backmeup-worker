package org.backmeup.job.impl;

import org.backmeup.model.BackupJob;

public interface JobReceiver {
	void start();
	void stop();
	void pause();
}
