package org.backmeup.service.client;

import java.util.Date;

import org.backmeup.model.BackupJob;
import org.backmeup.model.JobProtocol;
import org.backmeup.model.Status;

public interface BackmeupServiceFacade {
	Status saveStatus(Status status);
	void deleteStatusBefore(Long jobId, Date timeStamp);
	
	BackupJob findBackupJobById(String username, Long jobId);
	BackupJob saveBackupJob(BackupJob backupJob);
	
	JobProtocol saveJobProtocol(JobProtocol protocol);
	void deleteJobProtocolByUsername(String username);
	
	void sendEmail(String to, String subject, String message);
}
