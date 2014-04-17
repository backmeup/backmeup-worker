package org.backmeup.job.impl;

import java.util.EventObject;

import org.backmeup.model.BackupJob;

public class JobReceivedEvent extends EventObject {
	private static final long serialVersionUID = 6959348412326443090L;
	
	protected BackupJob backupJob;

	public JobReceivedEvent(Object obj, BackupJob backupJob) {
		super(obj);
		this.backupJob = backupJob;
	}

	public BackupJob getBackupJob() {
		return backupJob;
	}
}
