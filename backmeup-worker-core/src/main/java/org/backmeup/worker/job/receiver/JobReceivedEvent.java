package org.backmeup.worker.job.receiver;

import java.util.EventObject;

import org.backmeup.model.dto.BackupJobDTO;

public class JobReceivedEvent extends EventObject {
	private static final long serialVersionUID = 6959348412326443090L;
	
	protected BackupJobDTO backupJob;

	public JobReceivedEvent(Object obj, BackupJobDTO backupJob) {
		super(obj);
		this.backupJob = backupJob;
	}

	public BackupJobDTO getBackupJob() {
		return backupJob;
	}
}
