package org.backmeup.worker.job;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.backmeup.model.constants.JobExecutionStatus;
import org.backmeup.model.dto.BackupJobExecutionDTO;
import org.backmeup.model.dto.PluginProfileDTO;
import org.backmeup.model.spi.PluginDescribable;
import org.backmeup.plugin.api.Action;
import org.backmeup.plugin.api.ActionException;
import org.backmeup.plugin.api.Datasink;
import org.backmeup.plugin.api.Datasource;
import org.backmeup.plugin.api.Metadata;
import org.backmeup.plugin.api.PluginContext;
import org.backmeup.plugin.api.Progressable;
import org.backmeup.plugin.api.storage.Storage;
import org.backmeup.plugin.api.storage.StorageException;
import org.backmeup.plugin.infrastructure.PluginManager;
import org.backmeup.service.client.BackmeupService;
import org.backmeup.worker.perfmon.JobMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.monitor.Counter;

/**
 * Implements the actual BackupJob execution.
 */
public class BackupJobRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupJobRunner.class);

    private static final String INDEXING_PLUGIN_OSGI_BUNDLE_ID = "org.backmeup.indexing";
    private static final String INDEXER_BACKMEUP_PLUGIN_ID = "org.backmeup.indexer";

    private final String jobTempDir;
    private final String backupNameTemplate;

    private final PluginManager pluginManager;
    private final BackmeupService bmuService;

    private final Counter bytesReceived = JobMetrics.getCounter(BackupJobRunner.class, JobMetrics.BYTES_RECEIVED);
    private final Counter bytesSent = JobMetrics.getCounter(BackupJobRunner.class, JobMetrics.BYTES_SENT);
    private final Counter objectsReceived = JobMetrics.getCounter(BackupJobRunner.class, JobMetrics.OBJECTS_RECEIVED);
    private final Counter objectsSent = JobMetrics.getCounter(BackupJobRunner.class, JobMetrics.OBJECTS_SENT);

    public BackupJobRunner(PluginManager pluginManager, BackmeupService bmuService, String jobTempDir, String backupName) {
        this.pluginManager = pluginManager;
        this.bmuService = bmuService;
        this.jobTempDir = jobTempDir;
        this.backupNameTemplate = backupName;
    }

    public void executeBackup(Long jobExecutionId, Storage storage) throws StorageException {
        BackupJobExecutionDTO backupJob = this.bmuService.getBackupJobExecution(jobExecutionId, true);

        LOGGER.info("Job execution with id {} started for user {}", backupJob.getId(), backupJob.getUser().getUserId());

        backupJob.setStart(new Date());
        backupJob.setStatus(JobExecutionStatus.RUNNING);

        this.bmuService.updateBackupJobExecution(backupJob);

        try {
            // Open temporary local storage------------------------------------
            // This storage is used to temporarily store the data while executing the job
            String tmpDir = generateTmpDirName(backupJob, backupJob.getSource());
            storage.open(tmpDir);

            // Prepare context object -----------------------------------------
            // Make properties global for the action loop. So the plugins can 
            // communicate (e.g. filesplit and encryption plugins)
            PluginContext pluginContext = new PluginContext();
            pluginContext.setAttribute("org.backmeup.tmpdir", getLastSplitElement(tmpDir, "/"));
            pluginContext.setAttribute("org.backmeup.userid", backupJob.getUser().getUserId().toString());

            // TODO: Remove this workaround for indexing action
            pluginContext.setAttribute("org.backmeup.job", backupJob, true);

            // Prepare source plugin data -------------------------------------
            Datasource source = this.pluginManager.getDatasource(backupJob.getSource().getPluginId());

            // Prepare sink plugin data ---------------------------------------
            Datasink sink = this.pluginManager.getDatasink(backupJob.getSink().getPluginId());

            // Download from source -------------------------------------------
            LOGGER.info("Job {} downloading", backupJob.getId());
            source.downloadAll(backupJob.getSource(), pluginContext, storage, new JobStatusProgressor(backupJob, "datasource"));
            this.bytesReceived.increment(storage.getDataObjectSize());
            this.objectsReceived.increment(storage.getDataObjectCount());

            // if no actions are specified for this backup job,
            // initialize field with empty list
            if (backupJob.getActions() == null) {
                backupJob.setActions(new ArrayList<PluginProfileDTO>());
            }

            // add all properties which have been stored to the params collection
            for (PluginProfileDTO actionProfile : backupJob.getActions()) {
                //                Properties actionPropertiesProps = authenticationData.getByProfileId(actionProfile.getProfileId());
                //                Map<String, String> actionProperties = convertPropertiesToMap(actionPropertiesProps);
                //                params.putAll(actionProperties);
            }

            // Execute actions in sequence ------------------------------------
            LOGGER.info("Job {} processing", backupJob.getId());

            // Run indexing in case the user has enabled it using the 'enable.indexing' user property
            // We're using true as the default value for now
            boolean doIndexing = true;

            // has the indexer been requested during creation of the backup job?
            List<PluginProfileDTO> actions = backupJob.getActions();
            PluginProfileDTO indexer = null;
            for (PluginProfileDTO actionProfile : actions) {
                if (INDEXER_BACKMEUP_PLUGIN_ID.equals(actionProfile.getPluginId())) {
                    indexer = actionProfile;
                    break;
                }
            }

            if (doIndexing && indexer == null) {
                // if we need to index, add the indexer to the requested actions
                PluginDescribable ad = this.pluginManager.getPluginDescribableById(INDEXING_PLUGIN_OSGI_BUNDLE_ID);
                PluginProfileDTO indexActionProfile = new PluginProfileDTO();
                indexActionProfile.setPluginId(ad.getId());
                indexActionProfile.setProperties(new HashMap<String, String>());
                actions.add(indexActionProfile);
            }

            // The action maybe need to be sorted by their priority, e.g. to guarantee that encryption happens last
            // e.g. persistentJob.getSortedRequiredActions()
            for (PluginProfileDTO actionProfile : backupJob.getActions()) {
                String actionId = actionProfile.getPluginId();
                Action action;

                try {
                    LOGGER.info("Job {} processing action {}", backupJob.getId(), actionId);
                    if (INDEXER_BACKMEUP_PLUGIN_ID.equals(actionId)) {
                        if (doIndexing) {
                            // hand over information from the PluginDescribable to the indexAction plugin
                            PluginDescribable pluginDescr = this.pluginManager.getPluginDescribableById(backupJob.getSink().getPluginId());
                            Map<String, String> p = pluginDescr.getMetadata(backupJob.getSink().getAuthData().getProperties());

                            if ((p.get(Metadata.STORAGE_ALWAYS_ACCESSIBLE) != null) && (p.get(Metadata.DOWNLOAD_BASE) != null)) {
                                pluginContext.setAttribute(Metadata.STORAGE_ALWAYS_ACCESSIBLE, p.get(Metadata.STORAGE_ALWAYS_ACCESSIBLE));
                                pluginContext.setAttribute(Metadata.DOWNLOAD_BASE, p.get(Metadata.DOWNLOAD_BASE));
                            }
                            pluginContext.setAttribute("org.backmeup.bmuprefix", getLastSplitElement(tmpDir, "/"));
                            pluginContext.setAttribute("org.backmeup.thumbnails.tmpdir",
                                    "/data/thumbnails/" + getLastSplitElement(tmpDir, "/"));
                            doIndexing(actionProfile, pluginContext, storage, backupJob);
                        }
                    } else {
                        action = this.pluginManager.getAction(actionId);
                        action.doAction(actionProfile, pluginContext, storage, new JobStatusProgressor(backupJob, "action"));
                    }
                } catch (ActionException e) {
                    LOGGER.info("Job {} processing action {} failed with exception: {}", backupJob.getId(), actionId, e);
                }
            }

            // Upload to sink -------------------------------------------------
            LOGGER.info("Job {} uploading", backupJob.getId());
            sink.upload(backupJob.getSink(), pluginContext, storage, new JobStatusProgressor(backupJob, "datasink"));
            this.bytesSent.increment(storage.getDataObjectSize());
            this.objectsSent.increment(storage.getDataObjectCount());

            // Close temp local storage-----------------------------------------
            // Closing the storage means to remove all files in the temporary directory.
            // Including the root directory and the parent (/..../jobId/BMU_xxxxx)!
            // For debugging reasons, storage is not closed:
            //storage.close();

            backupJob.setStatus(JobExecutionStatus.SUCCESSFUL);

        } catch (Exception e) {
            LOGGER.error("Job {} failed with exception: {}", backupJob.getId(), e);

            backupJob.setStatus(JobExecutionStatus.ERROR);
        } finally {
            LOGGER.info("Job execution with id {} ended with status: {}", backupJob.getId(), backupJob.getStatus());
            backupJob.setEnd(new Date());
            this.bmuService.updateBackupJobExecution(backupJob);
        }
    }

    private void doIndexing(PluginProfileDTO profile, PluginContext context, Storage storage, BackupJobExecutionDTO job)
            throws ActionException, StorageException {
        // If we do indexing, the Thumbnail renderer needs to run before!
        Action thumbnailAction = this.pluginManager.getAction("org.backmeup.thumbnail");
        thumbnailAction.doAction(profile, context, storage, new JobStatusProgressor(job, "thumbnailAction"));

        // After thumbnail rendering, run indexing
        Action indexAction = this.pluginManager.getAction("org.backmeup.indexing");
        indexAction.doAction(profile, context, storage, new JobStatusProgressor(job, "indexaction"));
    }

    private String generateTmpDirName(BackupJobExecutionDTO job, PluginProfileDTO profile) {
        SimpleDateFormat formatter = null;
        Date date = new Date();

        Long profileid = profile.getProfileId();
        Long jobid = job.getId();
        // Take only last part of "org.backmeup.xxxx" (xxxx)
        String profilename = getLastSplitElement(profile.getPluginId(), "\\.");

        formatter = new SimpleDateFormat(this.backupNameTemplate.replaceAll("%PROFILEID%", profileid.toString()).replaceAll("%SOURCE%",
                profilename));

        return this.jobTempDir + "/" + jobid + "/" + formatter.format(date);
    }

    private String getLastSplitElement(String text, String regex) {
        String[] parts = text.split(regex);

        if (parts.length > 0) {
            return parts[parts.length - 1];
        } else {
            return text;
        }
    }

    private class JobStatusProgressor implements Progressable {

        private final BackupJobExecutionDTO job;
        private final String category;

        public JobStatusProgressor(BackupJobExecutionDTO job, String category) {
            this.job = job;
            this.category = category;
        }

        @Override
        public void progress(String message) {
            LOGGER.info("Job {} [{}] {}", this.job.getId(), this.category, message);
        }
    }
}
