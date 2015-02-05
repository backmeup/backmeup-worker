package org.backmeup.worker.job;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

import org.backmeup.keyserver.client.KeyserverFacade;
import org.backmeup.keyserver.model.AuthDataResult;
import org.backmeup.model.Token;
import org.backmeup.model.dto.BackupJobDTO;
import org.backmeup.model.dto.BackupJobDTO.JobStatus;
import org.backmeup.model.dto.JobProtocolDTO;
import org.backmeup.model.dto.PluginProfileDTO;
import org.backmeup.model.spi.PluginDescribable;
import org.backmeup.plugin.Plugin;
import org.backmeup.plugin.api.connectors.Action;
import org.backmeup.plugin.api.connectors.ActionException;
import org.backmeup.plugin.api.connectors.Datasink;
import org.backmeup.plugin.api.connectors.Datasource;
import org.backmeup.plugin.api.connectors.DatasourceException;
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

    private static final String ERROR_EMAIL_TEXT = "ERROR_EMAIL_TEXT";
    private static final String ERROR_EMAIL_SUBJECT = "ERROR_EMAIL_SUBJECT";
    private static final String ERROR_EMAIL_MIMETYPE = "ERROR_EMAIL_MIMETYPE";

    private static final String INDEXING_PLUGIN_OSGI_BUNDLE_ID = "org.backmeup.indexing";
    private static final String INDEXER_BACKMEUP_PLUGIN_ID = "org.backmeup.indexer";

    private final String jobTempDir;
    private final String backupName;

    private final Plugin plugins;
    private final KeyserverFacade keyserver;
    private final BackmeupService bmuService;

    private final ResourceBundle textBundle = ResourceBundle.getBundle("BackupJobRunner");

    public BackupJobRunner(Plugin plugins, KeyserverFacade keyserver, BackmeupService bmuService, String jobTempDir,
            String backupName) {
        this.plugins = plugins;
        this.keyserver = keyserver;
        this.bmuService = bmuService;
        this.jobTempDir = jobTempDir;
        this.backupName = backupName;
    }

    public void executeBackup(Long jobId, Storage storage) {

        BackupJobDTO backupJob = this.bmuService.getBackupJob(jobId);
        // when will the next access to the access data occur? current time + delay
        Long backupDate = new Date().getTime() + backupJob.getDelay();

        // get access data + new token for next access
        Token token = new Token(backupJob.getToken().getToken(), backupJob.getToken().getTokenId(), backupDate);
        AuthDataResult authenticationData = this.keyserver.getData(token);

        // the token for the next getData call
        Token newToken = authenticationData.getNewToken();

        // Set the new token information in the current job
        backupJob.getToken().setTokenId(newToken.getTokenId());
        backupJob.getToken().setToken(newToken.getToken());
        backupJob.getToken().setValidity(newToken.getBackupdate());

        backupJob.setJobStatus(JobStatus.running);

        // Store newToken for the next backup and set status to running
        this.bmuService.updateBackupJob(backupJob);

        //        Protocol Overview requires information about executed jobs
        //        JobProtocolDTO protocol = new JobProtocolDTO();
        //        protocol.setSinkTitle(persistentJob.getSink().getTitle());
        //        protocol.setExecutionTime(new Date().getTime());

        // track the error status messages
        //        List<JobStatus> errorStatus = new ArrayList<JobStatus>();

        Date jobStarted = new Date();

        // Open temporary storage
        try {
            Datasink sink = this.plugins.getDatasink(backupJob.getSink().getPluginId());
            Properties sinkAuthDataProps = authenticationData.getByProfileId(backupJob.getSink().getProfileId());
            Map<String, String> sinkAuthData = convertPropertiesToMap(sinkAuthDataProps);

            // delete previously stored status, as we only need the latest
            //deleteOldStatus(persistentJob);

            //addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.STARTED, StatusCategory.INFO, new Date().getTime()));
            LOGGER.info("Job " + backupJob.getJobId() + " started for userID:" + backupJob.getUser().getUserId());

            long previousSize = 0;

            PluginProfileDTO sourceProfile = backupJob.getSource();
            String tmpDir = generateTmpDirName(backupJob, sourceProfile);
            storage.open(tmpDir);

            Datasource source = this.plugins.getDatasource(sourceProfile.getPluginId());
            Properties sourceAuthDataProps = authenticationData.getByProfileId(sourceProfile.getProfileId());
            Map<String, String> sourceAuthData = convertPropertiesToMap(sourceAuthDataProps);

            // TODO: These should also come from Keyserver
            List<String> sourceOptions = new ArrayList<String>();
            if (sourceProfile.getOptions() != null) {
                sourceOptions.addAll(sourceProfile.getOptions());
            }

            // TODO: These should also come from Keyserver
            Map<String, String> sourceProperties = convertPropertiesToMap(sourceAuthDataProps);

            //addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.DOWNLOADING, StatusCategory.INFO, new Date().getTime()));
            LOGGER.info("Job " + backupJob.getJobId() + " downloading");

            // Download from source
            try {
                source.downloadAll(sourceAuthData, sourceProperties, sourceOptions, storage, new JobStatusProgressor(
                        backupJob, "datasource"));
            } catch (StorageException|DatasourceException e) {
                LOGGER.warn("Job " + backupJob.getJobId() + " failed at download", e);
                //errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.DOWNLOAD_FAILED, StatusCategory.WARNING, new Date().getTime(), e.getMessage())));
            }
            
            // for each datasource add an entry with bytes it consumed
            long currentSize = storage.getDataObjectSize() - previousSize;
            //protocol.addMember(new JobProtocolMemberDTO(protocol.getId(), "po.getProfile().getProfileName()", currentSize));
            //protocol.setSpace((int)currentSize);
            previousSize = storage.getDataObjectSize();

            // make properties global for the action loop. So the plugins can communicate (filesplitt + encryption)
            Map<String, String> params = new HashMap<>();
            params.putAll(sinkAuthData);
            params.putAll(sourceAuthData);

            // Execute Actions in sequence
            //addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.PROCESSING, StatusCategory.INFO, new Date().getTime()));
            LOGGER.info("Job " + backupJob.getJobId() + " processing");

            // if no actions are specified for this backup job,
            // initialize field with empty list
            if (backupJob.getActions() == null) {
                backupJob.setActions(new ArrayList<PluginProfileDTO>());
            }

            // add all properties which have been stored to the params collection
            for (PluginProfileDTO actionProfile : backupJob.getActions()) {
                //				params.putAll(actionProfile.);
                Properties actionPropertiesProps = authenticationData.getByProfileId(actionProfile.getProfileId());
                Map<String, String> actionProperties = convertPropertiesToMap(actionPropertiesProps);
                params.putAll(actionProperties);
            }

            // Run indexing in case the user has enabled it using the 'enable.indexing' user property
            // We're using true as the default value for now
            boolean doIndexing = true;

            String enableIndexing = null;
            // TODO Move the indexing property to datasource profile?
            //enableIndexing = persistentJob.getUser().getUserProperty("enable.indexing");
            if (enableIndexing != null) {
                if (enableIndexing.toLowerCase().trim().equals("false")) {
                    doIndexing = false;
                }
            }

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
                //TODO clarify actions.add(indexActionProfile) required here?
                actions.add(indexActionProfile);
            }

            // The action maybe need to be sorted by their priority, e.g. to guarantee that encryption happens last
            // e.g. persistentJob.getSortedRequiredActions()
            for (PluginProfileDTO actionProfile : backupJob.getActions()) {
                String actionId = actionProfile.getPluginId();
                Action action;
                try {
                    if ("org.backmeup.filesplitting".equals(actionId)) {
                        // If we do encryption, the Filesplitter needs to run before!
                        action = this.plugins.getAction(actionId);
                        //action.doAction(params, storage, persistentJob, new JobStatusProgressor(persistentJob, "filesplittaction"));
                    } else if ("org.backmeup.encryption".equals(actionId)) {
                        // Add the encryption password to the parameters
                        if (authenticationData.getEncryptionPwd() != null) {
                            params.put("org.backmeup.encryption.password", authenticationData.getEncryptionPwd());
                        }

                        // After splitting, run encryption
                        action = this.plugins.getAction(actionId);
                        //action.doAction(params, storage, persistentJob,	new JobStatusProgressor(persistentJob, "encryptionaction"));
                    } else if (INDEXER_BACKMEUP_PLUGIN_ID.equals(actionId)) {
                        // Do nothing - we ignore index action declaration in the job description and use
                        // the info from the user properties instead
                        if (doIndexing) {
                            //hand over information from PluginDescribable, etc. to indexAction
                            PluginDescribable pluginDescr = this.plugins.getPluginDescribableById(backupJob.getSink()
                                    .getPluginId());
                            Map<String, String> p = pluginDescr.getMetadata(sinkAuthData);
                            //add the BMU_filegenerator_530_26_01_2015_12_56 prefix for indexer plugin
                            p.put("org.backmeup.bmuprefix", getLastSplitElement(tmpDir, "/"));
                            //add 
                            p.put("org.backmeup.thumbnails.tmpdir",
                                    "/data/thumbnails/" + getLastSplitElement(tmpDir, "/"));
                            doIndexing(p, params, storage, backupJob);
                        }

                    } else {
                        // Only happens in case Job was corrupted in the core - we'll handle that as a fatal error
                        LOGGER.error("Job " + backupJob.getJobId() + " failed with unsupported action: " + actionId);
                        //errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), "Unsupported Action: " + actionId)));
                    }
                } catch (ActionException e) {
                    // Should only happen in case of problems in the backmeup-service (file I/O, DB access, etc.)
                    // We'll handle that as a fatal error
                    LOGGER.error("Job " + backupJob.getJobId() + " failed at executing an action", e);
                    //errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), e.getMessage())));
                }
            }

            try {
                // Upload to Sink
                //addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.UPLOADING, StatusCategory.INFO, new Date().getTime()));
                LOGGER.info("Job " + backupJob.getJobId() + " uploading");

                sinkAuthData.put("org.backmeup.tmpdir", getLastSplitElement(tmpDir, "/"));
                sinkAuthData.put("org.backmeup.userid", backupJob.getUser().getUserId() + "");

                //TODO: Separate sinkAuthData and sinkProperties
                Map<String, String> sinkProperties = convertPropertiesToMap(sinkAuthDataProps);
                List<String> sinkOptions = new ArrayList<>();
                sink.upload(sinkAuthData, sinkProperties, sinkOptions, storage, new JobStatusProgressor(backupJob,
                        "datasink"));

                //addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.SUCCESSFUL, StatusCategory.INFO, new Date().getTime()));
                LOGGER.info("Job " + backupJob.getJobId() + " successful");
            } catch (StorageException e) {
                LOGGER.error("Job " + backupJob.getJobId() + " failed at upload", e);
                //errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), e.getMessage())));
            }

            // store job protocol within database

            // Closing the storage means to remove all files in the temporary directory.
            // Including the root directory and the parent (/..../jobId/BMU_xxxxx)!
            // For debugging reasons, storage is not closed:
            //storage.close();

            JobProtocolDTO protocol = new JobProtocolDTO();
            protocol.setTimestamp(new Date().getTime());
            protocol.setStart(jobStarted.getTime());
            protocol.setExecutionTime(protocol.getTimestamp() - protocol.getStart());
            protocol.setSuccessful(true);
            protocol.setProcessedItems(storage.getDataObjectCount());
            protocol.setSpace((int) currentSize);

            backupJob.addProtocol(protocol);
            backupJob.setJobStatus(JobStatus.successful);

        } catch (Exception e) {
            LOGGER.error("Job " + backupJob.getJobId() + " failed with unhandled exception", e);
            //storeJobProtocol(backupJob, protocol, 0, false);
            //errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), e.getMessage())));

            int processedItems = 0;
            try {
                processedItems = storage.getDataObjectCount();
            } catch (StorageException e1) {
                LOGGER.error("", e);
            }

            JobProtocolDTO protocol = new JobProtocolDTO();
            protocol.setTimestamp(new Date().getTime());
            protocol.setStart(jobStarted.getTime());
            protocol.setExecutionTime(protocol.getTimestamp() - protocol.getStart());
            protocol.setSuccessful(false);
            protocol.setProcessedItems(processedItems);
            protocol.setSpace(0);
            protocol.setMessage(e.toString());

            backupJob.addProtocol(protocol);
            backupJob.setJobStatus(JobStatus.error);
        }

        // send error message, if there were any error status messages
        //        if (!errorStatus.isEmpty()) {
        //            bmuService.sendEmail(job.getUser().getEmail(), MessageFormat.format(
        //                    textBundle.getString(ERROR_EMAIL_SUBJECT), job.getUser().getEmail()),
        //                    MessageFormat.format(
        //                            textBundle.getString(ERROR_EMAIL_TEXT), job.getUser().getEmail(),
        //                            job.getJobTitle()));
        //        }

        this.bmuService.updateBackupJob(backupJob);
    }

    private void doIndexing(Map<String, String> properties, Map<String, String> params, Storage storage, BackupJobDTO job)
            throws ActionException {
        // If we do indexing, the Thumbnail renderer needs to run before!
        Action thumbnailAction = this.plugins.getAction("org.backmeup.thumbnail");
        thumbnailAction.doAction(null, properties, null, storage, job, new JobStatusProgressor(job, "thumbnailAction"));

        // After thumbnail rendering, run indexing
        Action indexAction = this.plugins.getAction("org.backmeup.indexing");
        indexAction.doAction(null, properties, null, storage, job, new JobStatusProgressor(job, "indexaction"));
    }

    //    private JobStatus addStatusToDb(JobStatus status) {
    //        logger.debug("Job status: {}", status.getMessage());
    //        bmuService.saveStatus(status);
    //        return status;
    //    }
    //
    //    private void deleteOldStatus(BackupJobDTO persistentJob) {
    //        bmuService.deleteStatusBefore(persistentJob.getJobId(), new Date());
    //    }
    //
    //    private void storeJobProtocol(BackupJobDTO job, JobProtocolDTO protocol, int storedEntriesCount, boolean success) {
    //        // remove old entries, then store the new one
    //        bmuService.deleteJobProtocolByUsername(job.getUser().getUsername());
    //
    //        protocol.setUser(job.getUser());
    //        protocol.setJobId(job.getJobId());
    //        protocol.setSuccessful(success);
    //        protocol.setProcessedItems(storedEntriesCount);
    //
    //        if (protocol.isSuccessful()) {
    //            job.setLastSuccessful(protocol.getExecutionTime());
    //            job.setJobStatus(JobStatus.successful);
    //        } else {
    //            job.setLastFailed(protocol.getExecutionTime());
    //            job.setJobStatus(JobStatus.error);
    //        }
    //
    //        bmuService.saveJobProtocol(job.getUser().getUsername(), job.getJobId(), protocol);
    //    }

    private String generateTmpDirName(BackupJobDTO job, PluginProfileDTO profile) {
        SimpleDateFormat formatter = null;
        Date date = new Date();

        Long profileid = profile.getProfileId();
        Long jobid = job.getJobId();
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

        private final BackupJobDTO job;
        private final String category;

        public JobStatusProgressor(BackupJobDTO job, String category) {
            this.job = job;
            this.category = category;
        }

        @Override
        public void progress(String message) {
            LOGGER.info("Job {} [{}] {}", this.job.getJobId(), this.category, message);
            //addStatusToDb(new JobStatus(job.getJobId(), "info", category, new Date().getTime(),  message));
        }
    }
}
