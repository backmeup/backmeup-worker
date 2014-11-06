package org.backmeup.worker.job;

import org.backmeup.keyserver.client.KeyserverFacade;
import org.backmeup.keyserver.client.impl.KeyserverClient;
import org.backmeup.plugin.Plugin;
import org.backmeup.plugin.api.storage.Storage;
import org.backmeup.service.client.BackmeupServiceFacade;
import org.backmeup.service.client.impl.BackmeupServiceClient;
import org.backmeup.worker.plugin.osgi.PluginImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupJobWorkerThread implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(BackupJobWorkerThread.class);
	
	private final Long backupJobId;
	private final String backupName;
	
	private final String jobTempDir;
	
	private final String keyserverScheme;
	private final String keyserverHost;
	private final String keyserverPath;
	
	private final String bmuServiceScheme;
	private final String bmuServiceHost;
	private final String bmuServicePath;
	private final String bmuServiceAccessToken;
	
	private final Plugin plugins;
	private final KeyserverFacade keyserverClient;
	private final BackmeupServiceFacade bmuServiceClient;

	public BackupJobWorkerThread(Long backupJobId, Plugin plugins,
			String serviceScheme, String serviceHost, String servicePath,
			String serviceAccessToken, String keyserverScheme,
			String keyserverHost, String keyserverPath, String jobTempDir,
			String backupName) {
		super();
		this.backupJobId = backupJobId;
		this.backupName = backupName;
		this.jobTempDir = jobTempDir;
		this.plugins = plugins;
		this.keyserverScheme = keyserverScheme;
		this.keyserverHost = keyserverHost;
		this.keyserverPath = keyserverPath;
		this.bmuServiceScheme = serviceScheme;
		this.bmuServiceHost = serviceHost;
		this.bmuServicePath = servicePath;
		this.bmuServiceAccessToken = serviceAccessToken;
		
		this.keyserverClient = new KeyserverClient(this.keyserverScheme, this.keyserverHost, this.keyserverPath); 
		this.bmuServiceClient = new BackmeupServiceClient(bmuServiceScheme, bmuServiceHost, bmuServicePath, bmuServiceAccessToken);
	}

	@Override
	public void run() {
		try {
			BackupJobRunner runner = new BackupJobRunner(plugins, keyserverClient, bmuServiceClient, jobTempDir, backupName);
			Storage storage = ((PluginImpl)plugins).service(Storage.class, "(name=" + "org.backmeup.localfilesystemstorage" + ")");
			runner.executeBackup(backupJobId, storage);
		} catch (Exception e) {
			LOGGER.error("Failed to process job", e);
		}
	}
}
