package org.backmeup.job.impl;

import org.backmeup.keyserver.client.KeyserverFacade;
import org.backmeup.keyserver.client.impl.KeyserverClient;
import org.backmeup.model.BackupJob;
import org.backmeup.plugin.Plugin;
import org.backmeup.plugin.api.storage.Storage;
import org.backmeup.service.client.BackmeupServiceFacade;
import org.backmeup.service.client.impl.BackmeupServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupJobWorkerThread implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(BackupJobWorkerThread.class);
	
	private BackupJob backupJob;
	
	private String indexHost;
	private int indexPort;
	private String jobTempDir;
	private String backupName;
	
	private Plugin plugins;
	private KeyserverFacade keyserverClient;
	private BackmeupServiceFacade bmuServiceClient;

	public BackupJobWorkerThread(BackupJob backupJob, Plugin plugins, String indexHost, int indexPort, String jobTempDir, String backupName) {
		super();
		this.backupJob = backupJob;
		this.plugins = plugins;
		this.indexHost = indexHost;
		this.indexPort = indexPort;
		this.jobTempDir = jobTempDir;
		this.backupName = backupName;
		
		this.keyserverClient = new KeyserverClient(); // TODO
		this.bmuServiceClient = new BackmeupServiceClient(); // TODO
	}

	@Override
	public void run() {
		try {
			BackupJobRunner runner = new BackupJobRunner(plugins, keyserverClient, bmuServiceClient, indexHost, indexPort, jobTempDir, backupName);
			// TODO: create new instance of LocalFilesystemStorage from osgi container (plugins)
			// Storage storage = new LocalFilesystemStorage();
			Storage storage = null;
			runner.executeBackup(backupJob, storage);
		} catch (Exception e) {
			logger.error("Failed to process job", e);
		}
		
	}

}
