package org.backmeup.service.client;

import java.util.Date;

import org.backmeup.model.Status;
import org.backmeup.model.dto.Job;
import org.backmeup.model.dto.JobProtocolDTO;
import org.backmeup.model.dto.JobStatus;

public interface BackmeupServiceFacade {
	Status saveStatus(JobStatus status);
	void deleteStatusBefore(Long jobId, Date timeStamp);
	
	Job findBackupJobById(String username, Long jobId);
//	Job saveBackupJob(Job backupJob);
	
	void saveJobProtocol(String username, Long jobId, JobProtocolDTO protocol);
	void deleteJobProtocolByUsername(String username);
	
	void sendEmail(String to, String subject, String message);
	
}
