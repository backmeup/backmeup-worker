package org.backmeup.worker.job;

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
    private final BackmeupService bmuServiceClient;

    public BackupJobWorkerThread(Long backupJobId, Plugin plugins,
            String serviceScheme, String serviceHost, String servicePath,
            String serviceAccessToken, String jobTempDir, String backupName) {
        this(backupJobId, plugins, new BackmeupServiceClient(serviceScheme,
                serviceHost, servicePath, serviceAccessToken), jobTempDir,
                backupName);
    }

    public BackupJobWorkerThread(Long backupJobId, Plugin plugins,
            BackmeupService bmuServiceClient, String jobTempDir,String backupName) {
        super();
        this.backupJobId = backupJobId;
        this.backupName = backupName;
        this.jobTempDir = jobTempDir;
        this.plugins = plugins;

        this.bmuServiceClient = bmuServiceClient;
    }

    @Override
    public void run() {
        try {
            BackupJobRunner runner = new BackupJobRunner(plugins, bmuServiceClient, jobTempDir, backupName);
            Storage storage = ((PluginImpl)plugins).service(Storage.class, "org.backmeup.localfilesystemstorage");
            runner.executeBackup(backupJobId, storage);
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new WorkerException("Failed to process job", e);
        }
    }
}
