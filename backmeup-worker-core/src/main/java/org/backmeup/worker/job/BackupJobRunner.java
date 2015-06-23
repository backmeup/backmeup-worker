package org.backmeup.worker.job;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.backmeup.model.constants.JobExecutionStatus;
import org.backmeup.model.dto.BackupJobExecutionDTO;
import org.backmeup.model.dto.PluginProfileDTO;
import org.backmeup.model.spi.PluginDescribable;
import org.backmeup.plugin.Plugin;
import org.backmeup.plugin.api.connectors.Action;
import org.backmeup.plugin.api.connectors.ActionException;
import org.backmeup.plugin.api.connectors.Datasink;
import org.backmeup.plugin.api.connectors.Datasource;
import org.backmeup.plugin.api.connectors.Progressable;
import org.backmeup.plugin.api.storage.Storage;
import org.backmeup.plugin.api.storage.StorageException;
import org.backmeup.service.client.BackmeupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the actual BackupJob execution.
 */
@SuppressWarnings(value = { "all" })
public class BackupJobRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupJobRunner.class);

    private static final String INDEXING_PLUGIN_OSGI_BUNDLE_ID = "org.backmeup.indexing";
    private static final String INDEXER_BACKMEUP_PLUGIN_ID = "org.backmeup.indexer";

    private final String jobTempDir;
    private final String backupName;

    private final Plugin plugins;
    private final BackmeupService bmuService;

    public BackupJobRunner(Plugin plugins, BackmeupService bmuService, String jobTempDir, String backupName) {
        this.plugins = plugins;
        this.bmuService = bmuService;
        this.jobTempDir = jobTempDir;
        this.backupName = backupName;
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
            
            // Prepare source plugin data -------------------------------------
            
            Datasource source = this.plugins.getDatasource(backupJob.getSource().getPluginId());
            Map<String, String> sourceAuthData = new HashMap<String, String>();
            if (backupJob.getSource().getAuthData() != null) {
                sourceAuthData = backupJob.getSource().getAuthData().getProperties();
            }
            Map<String, String> sourceProperties = backupJob.getSource().getProperties();
            if(sourceProperties == null) sourceProperties = new HashMap<String, String>();
            List<String> sourceOptions = backupJob.getSource().getOptions();
            if(sourceOptions == null) sourceOptions = new ArrayList<String>();
            

            // Prepare sink plugin data ---------------------------------------
            
            Datasink sink = this.plugins.getDatasink(backupJob.getSink().getPluginId());
            Map<String, String> sinkAuthData = new HashMap<String, String>();
            if (backupJob.getSink().getAuthData() != null) {
                sinkAuthData = backupJob.getSink().getAuthData().getProperties();
            }
            
            sinkAuthData.put("org.backmeup.tmpdir", getLastSplitElement(tmpDir, "/"));
            sinkAuthData.put("org.backmeup.userid", backupJob.getUser().getUserId() + "");

            Map<String, String> sinkProperties = backupJob.getSink().getProperties();
            if(sinkProperties == null) sinkProperties = new HashMap<String, String>();
            List<String> sinkOptions = backupJob.getSink().getOptions();
            if(sinkOptions == null) sinkOptions = new ArrayList<String>();
            

            // Download from source -------------------------------------------
            LOGGER.info("Job {} downloading", backupJob.getId());
            source.downloadAll(sourceAuthData, sourceProperties, sourceOptions, storage, 
                    new JobStatusProgressor(backupJob, "datasource"));
            
            // Prepare plugin data for actions --------------------------------
            // Make properties global for the action loop. So the plugins can 
            // communicate (e.g. filesplit and encryption plugins)
            Map<String, String> params = new HashMap<>();
            params.putAll(sinkAuthData);
            params.putAll(sourceAuthData);
            
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
            boolean doIndexing = false;

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
                PluginDescribable ad = this.plugins.getPluginDescribableById(INDEXING_PLUGIN_OSGI_BUNDLE_ID);
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
                            //hand over information from PluginDescribable, etc. to indexAction
                            PluginDescribable pluginDescr = this.plugins.getPluginDescribableById(backupJob.getSink().getPluginId());
                            Map<String, String> p = pluginDescr.getMetadata(sinkAuthData);
                            //add the BMU_filegenerator_530_26_01_2015_12_56 prefix for indexer plugin
                            p.put("org.backmeup.bmuprefix", getLastSplitElement(tmpDir, "/"));
                            //add 
                            p.put("org.backmeup.thumbnails.tmpdir",
                                    "/data/thumbnails/" + getLastSplitElement(tmpDir, "/"));
                            doIndexing(p, params, storage, backupJob);
                        }
                    } else {
                        action = this.plugins.getAction(actionId);
                        action.doAction(null, params, null, storage, backupJob, new JobStatusProgressor(backupJob, "action"));
                    }
                } catch (ActionException e) {
                    LOGGER.info("Job {} processing action {} failed with exception: {}", backupJob.getId(), actionId, e);
                }
            }

            // Upload to sink -------------------------------------------------
            LOGGER.info("Job {} uploading", backupJob.getId());
            sink.upload(sinkAuthData, sinkProperties, sinkOptions, storage, 
                    new JobStatusProgressor(backupJob, "datasink"));

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

    private void doIndexing(Map<String, String> properties, Map<String, String> params, Storage storage, BackupJobExecutionDTO job)
            throws ActionException {
        // If we do indexing, the Thumbnail renderer needs to run before!
        Action thumbnailAction = this.plugins.getAction("org.backmeup.thumbnail");
        thumbnailAction.doAction(null, properties, null, storage, job, new JobStatusProgressor(job, "thumbnailAction"));

        // After thumbnail rendering, run indexing
        Action indexAction = this.plugins.getAction("org.backmeup.indexing");
        indexAction.doAction(null, properties, null, storage, job, new JobStatusProgressor(job, "indexaction"));
    }

    private String generateTmpDirName(BackupJobExecutionDTO job, PluginProfileDTO profile) {
        SimpleDateFormat formatter = null;
        Date date = new Date();

        Long profileid = profile.getProfileId();
        Long jobid = job.getId();
        // Take only last part of "org.backmeup.xxxx" (xxxx)
        String profilename = getLastSplitElement(profile.getPluginId(), "\\.");

        formatter = new SimpleDateFormat(this.backupName.replaceAll("%PROFILEID%", profileid.toString()).replaceAll(
                "%SOURCE%", profilename));

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
    
    private Map<String, String> convertPropertiesToMap (Properties properties) {
        Map<String, String> map = new HashMap<>();
        for (final String name: properties.stringPropertyNames()) {
            map.put(name, properties.getProperty(name));
        }
        return map;
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
