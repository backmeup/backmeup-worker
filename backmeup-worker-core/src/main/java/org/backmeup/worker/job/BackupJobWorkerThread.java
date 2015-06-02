package org.backmeup.worker.job;

import org.backmeup.keyserver.client.KeyserverClient;
import org.backmeup.plugin.Plugin;
import org.backmeup.plugin.api.storage.Storage;
import org.backmeup.service.client.BackmeupService;
import org.backmeup.service.client.impl.BackmeupServiceClient;
import org.backmeup.worker.WorkerException;
import org.backmeup.worker.plugin.osgi.PluginImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackupJobWorkerThread implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupJobWorkerThread.class);

    private final Long backupJobId;
    private final String backupName;

    private final String jobTempDir;

    private final Plugin plugins;
    private final KeyserverClient keyserverClient;
    private final BackmeupService bmuServiceClient;

    public BackupJobWorkerThread(Long backupJobId, Plugin plugins,
            String serviceScheme, String serviceHost, String servicePath,
            String serviceAccessToken, String keyserverScheme,
            String keyserverHost, String keyserverPath, String jobTempDir,
            String backupName) {
        this(backupJobId, 
                plugins, 
                new BackmeupServiceClient(serviceScheme, serviceHost, servicePath, serviceAccessToken), 
                new KeyserverClient(keyserverScheme, keyserverHost, keyserverPath), 
                jobTempDir, 
                backupName);
    }
    
    public BackupJobWorkerThread(Long backupJobId, Plugin plugins,
            BackmeupService bmuServiceClient, KeyserverClient keyserverClient, 
            String jobTempDir, String backupName) {
        super();
        this.backupJobId = backupJobId;
        this.backupName = backupName;
        this.jobTempDir = jobTempDir;
        this.plugins = plugins;

        this.keyserverClient = keyserverClient; 
        this.bmuServiceClient = bmuServiceClient;
    }

    @Override
    public void run() {
        try {
            BackupJobRunner runner = new BackupJobRunner(plugins, keyserverClient, bmuServiceClient, jobTempDir, backupName);
            Storage storage = ((PluginImpl)plugins).service(Storage.class, "(name=" + "org.backmeup.localfilesystemstorage" + ")");
            runner.executeBackup(backupJobId, storage);
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new WorkerException("Failed to process job", e);
        }
    }
}
