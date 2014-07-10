package org.backmeup.worker.job;

import org.backmeup.keyserver.client.KeyserverFacade;
import org.backmeup.keyserver.client.impl.KeyserverClient;
import org.backmeup.model.dto.BackupJobDTO;
import org.backmeup.plugin.Plugin;
import org.backmeup.plugin.api.storage.Storage;
import org.backmeup.service.client.BackmeupServiceFacade;
import org.backmeup.service.client.impl.BackmeupServiceClient;
import org.backmeup.worker.plugin.osgi.PluginImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupJobWorkerThread implements Runnable {
	private final Logger logger = LoggerFactory.getLogger(BackupJobWorkerThread.class);
	
	private final BackupJobDTO backupJob;
	private final String backupName;
	
	private final String indexHost;
	private final int indexPort;
	private final String jobTempDir;
	
	
	private final Plugin plugins;
	private final KeyserverFacade keyserverClient;
	private final BackmeupServiceFacade bmuServiceClient;

	public BackupJobWorkerThread(BackupJobDTO backupJob, Plugin plugins, String indexHost, int indexPort, String jobTempDir, String backupName) {
		super();
		this.backupJob = backupJob;
		this.backupName = backupName;
		this.jobTempDir = jobTempDir;
		this.plugins = plugins;
		this.indexHost = indexHost;
		this.indexPort = indexPort;
		
		this.keyserverClient = new KeyserverClient("http", "localhost:8080", "/backmeup-keyserver"); // TODO
		this.bmuServiceClient = new BackmeupServiceClient("http", "localhost", "8080", "/backmeup-service-rest/backmeup"); // TODO
	}

	@Override
	public void run() {
		try {
			BackupJobRunner runner = new BackupJobRunner(plugins, keyserverClient, bmuServiceClient, indexHost, indexPort, jobTempDir, backupName);
			Storage storage = ((PluginImpl)plugins).service(Storage.class, "(name=" + "org.backmeup.localfilesystemstorage" + ")");
			runner.executeBackup(backupJob, storage);
		} catch (Exception e) {
			logger.error("Failed to process job", e);
		}
	}
}
