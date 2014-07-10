package org.backmeup.service.client;

import org.backmeup.model.dto.BackupJobDTO;

public interface BackmeupServiceFacade {
//	Status saveStatus(JobStatus status);
//	void deleteStatusBefore(Long jobId, Date timeStamp);
	
	BackupJobDTO getBackupJob(Long jobId);
	BackupJobDTO updateBackupJob(BackupJobDTO backupJob);
	
//	void saveJobProtocol(String username, Long jobId, JobProtocolDTO protocol);
//	void deleteJobProtocolByUsername(String username);
	
//	void sendEmail(String to, String subject, String message);
	
}
