package org.backmeup.worker.job;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.ResourceBundle;

import org.backmeup.keyserver.client.KeyserverFacade;
import org.backmeup.keyserver.model.AuthDataResult;
import org.backmeup.model.StatusCategory;
import org.backmeup.model.StatusType;
import org.backmeup.model.Token;
import org.backmeup.model.constants.BackupJobStatus;
import org.backmeup.model.dto.BackupJobDTO;
import org.backmeup.model.dto.BackupJobDTO.JobStatus;
import org.backmeup.model.dto.JobProtocolDTO;
import org.backmeup.model.dto.PluginProfileDTO;
import org.backmeup.model.spi.ActionDescribable;
import org.backmeup.plugin.Plugin;
import org.backmeup.plugin.api.actions.Action;
import org.backmeup.plugin.api.actions.ActionException;
/*
import org.backmeup.plugin.api.actions.encryption.EncryptionAction;
import org.backmeup.plugin.api.actions.filesplitting.FilesplittAction;
import org.backmeup.plugin.api.actions.indexing.IndexAction;
import org.backmeup.plugin.api.actions.indexing.IndexDescribable;
import org.backmeup.plugin.api.actions.thumbnail.ThumbnailAction;
*/
import org.backmeup.plugin.api.connectors.Datasink;
import org.backmeup.plugin.api.connectors.Datasource;
import org.backmeup.plugin.api.connectors.DatasourceException;
import org.backmeup.plugin.api.connectors.Progressable;
import org.backmeup.plugin.api.storage.Storage;
import org.backmeup.plugin.api.storage.StorageException;
import org.backmeup.service.client.BackmeupServiceFacade;
import org.backmeup.worker.plugin.osgi.PluginImpl;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the actual BackupJob execution.
 */
@SuppressWarnings(value = { "all" })
public class BackupJobRunner {
	private final Logger logger = LoggerFactory.getLogger(BackupJobRunner.class);

	private static final String ERROR_EMAIL_TEXT = "ERROR_EMAIL_TEXT";
	private static final String ERROR_EMAIL_SUBJECT = "ERROR_EMAIL_SUBJECT";
	private static final String ERROR_EMAIL_MIMETYPE = "ERROR_EMAIL_MIMETYPE";

	private final String indexHost;
	private final int    indexPort;
	private final String indexName = "es-backmeup-cluster";

	private final String jobTempDir;
	private final String backupName;

	private final Plugin plugins;
	private final KeyserverFacade keyserver;
	private final BackmeupServiceFacade bmuService;

	private final ResourceBundle textBundle = ResourceBundle.getBundle("BackupJobRunner");

	public BackupJobRunner(Plugin plugins, KeyserverFacade keyserver, BackmeupServiceFacade bmuService, String indexHost, int indexPort, String jobTempDir, String backupName) {
		this.plugins = plugins;
		this.keyserver = keyserver;
		this.bmuService = bmuService;
		this.indexHost = indexHost;
		this.indexPort = indexPort;
		this.jobTempDir = jobTempDir;
		this.backupName = backupName;
	}

	public void executeBackup(Long jobId, Storage storage) {

		// use the job which is stored within the database
		BackupJobDTO backupJob = bmuService.getBackupJob(jobId);
		// when will the next access to the access data occur? current time + delay
		Long backupDate = new Date().getTime() + backupJob.getDelay();
//		persistentJob.setBackupDate(new Date().getTime() + persistentJob.getDelay());

		// get access data + new token for next access
		Token token = new Token(
				backupJob.getToken().getToken(),
				backupJob.getToken().getTokenId(),
				backupDate);
		AuthDataResult authenticationData = keyserver.getData(token);

		// the token for the next getData call
		Token newToken = authenticationData.getNewToken();
		
		// Set the new token information in the current job
		backupJob.getToken().setTokenId(newToken.getTokenId());
		backupJob.getToken().setToken(newToken.getToken());
		backupJob.getToken().setValidity(newToken.getBackupdate());
		
		backupJob.setJobStatus(JobStatus.running);

		// Store newToken for the next backup and set status to running
		bmuService.updateBackupJob(backupJob);

//		// Protocol Overview requires information about executed jobs
//		JobProtocolDTO protocol = new JobProtocolDTO();
////		protocol.setSinkTitle(persistentJob.getSink().getTitle());
//		protocol.setExecutionTime(new Date().getTime());

		// track the error status messages
//		List<JobStatus> errorStatus = new ArrayList<JobStatus>();
		
		Date jobStarted = new Date();
		
		// Open temporary storage
		try {
			Datasink sink = plugins.getDatasink(backupJob.getSink().getPluginId());
			Properties sinkProperties = authenticationData.getByProfileId(backupJob.getSink().getProfileId());

			// delete previously stored status, as we only need the latest
//			deleteOldStatus(persistentJob);
			
//			addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.STARTED, StatusCategory.INFO, new Date().getTime()));
			logger.info("Job " + backupJob.getJobId() + " startet");
			
			long previousSize = 0;

			PluginProfileDTO sourceProfile = backupJob.getSource();
			String tmpDir = generateTmpDirName(backupJob, sourceProfile);
			storage.open(tmpDir);

			Datasource source = plugins.getDatasource(sourceProfile.getPluginId());

			// Properties sourceProperties = authenticationData.getByProfileId(po.getProfile().getProfileId());
			Properties sourceProperties = authenticationData.getByProfileId(sourceProfile.getProfileId());

			List<String> sourceOptions = new ArrayList<String>();
			if (sourceProfile.getOptions() != null) {
				sourceOptions.addAll(sourceProfile.getOptions());
			}

//			addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.DOWNLOADING, StatusCategory.INFO, new Date().getTime()));
			logger.info("Job " + backupJob.getJobId() + " downloading");
			
			// Download from source
			try {
				source.downloadAll(sourceProperties, sourceOptions, storage, new JobStatusProgressor(backupJob, "datasource"));
			} catch (StorageException e) {
//				logger.error("", e);
//				errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.DOWNLOAD_FAILED, StatusCategory.WARNING, new Date().getTime(), e.getMessage())));
				logger.warn("Job " + backupJob.getJobId() + " faild with message: " + e);
			} catch (DatasourceException e) {
//				logger.error("", e);
//				errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.DOWNLOAD_FAILED, StatusCategory.WARNING, new Date().getTime(), e.getMessage())));
				logger.warn("Job " + backupJob.getJobId() + " faild with message: " + e);
			}

			// for each datasource add an entry with bytes it consumed
			long currentSize = storage.getDataObjectSize() - previousSize;
//			protocol.addMember(new JobProtocolMemberDTO(protocol.getId(), "po.getProfile().getProfileName()", currentSize));
//			protocol.setSpace((int)currentSize);
			previousSize = storage.getDataObjectSize();

			// make properties global for the action loop. So the plugins can communicate (filesplitt + encryption)
			Properties params = new Properties();
			params.putAll(sinkProperties);
			params.putAll(sourceProperties);

			// Execute Actions in sequence
//			addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.PROCESSING, StatusCategory.INFO, new Date().getTime()));
			logger.info("Job " + backupJob.getJobId() + " processing");
			
			// if no actions are specified for this backup job,
			// initialize field with empty list
			if(backupJob.getActions() == null){
				backupJob.setActions(new ArrayList<PluginProfileDTO>());
			}

			// add all properties which have been stored to the params collection
			for (PluginProfileDTO actionProfile : backupJob.getActions()) {
//				params.putAll(actionProfile.);
				Properties actionProperties = authenticationData.getByProfileId(actionProfile.getProfileId());
				params.putAll(actionProperties);
			}

			
			// Run indexing in case the user has enabled it using the 'enable.indexing' user property
			// We're using true as the default value for now
			boolean doIndexing = false; 

			String enableIndexing = null;
			// Move the indexing property to datasource profile?
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
				if ("org.backmeup.indexer".equals(actionProfile.getPluginId())) {
					indexer = actionProfile;
					break;
				}
			}

			if (doIndexing && indexer == null) {
				// if we need to index, add the indexer to the requested actions
				ActionDescribable ad = plugins.getActionById("org.backmeup.indexer");
				PluginProfileDTO indexActionProfile = new PluginProfileDTO();
				indexActionProfile.setPluginId(ad.getId());
				indexActionProfile.setConfigProperties(new HashMap<String, String>());
//				actions.add(new PluginProfileDTO(ad.getId(), ad.getPriority(), new HashMap<String, String>()));
			}

			// The action maybe need to be sorted by their priority, e.g. to guarantee that encryption happens last
			// e.g. persistentJob.getSortedRequiredActions()
			for (PluginProfileDTO actionProfile : backupJob.getActions()) {
				String actionId = actionProfile.getPluginId();
				Action action;
				Client client = null;

				try {
					if ("org.backmeup.filesplitting".equals(actionId)) {
						// If we do encryption, the Filesplitter needs to run before!
						action = ((PluginImpl)plugins).getAction(actionId);
						//							action.doAction(params, storage, persistentJob, new JobStatusProgressor(persistentJob, "filesplittaction"));
					} else if ("org.backmeup.encryption".equals(actionId)) {
						// Add the encryption password to the parameters
						if (authenticationData.getEncryptionPwd() != null) {
							params.put("org.backmeup.encryption.password", authenticationData.getEncryptionPwd());
						}

						// After splitting, run encryption
						action = ((PluginImpl)plugins).getAction(actionId);
						//							action.doAction(params, storage, persistentJob,	new JobStatusProgressor(persistentJob, "encryptionaction"));
					} else if ("org.backmeup.indexer".equals(actionId)) {
						// Do nothing - we ignore index action declaration in the job description and use
						// the info from the user properties instead
						if (doIndexing) {
							doIndexing(params, storage, backupJob, client);
						}

					} else {
						// Only happens in case Job was corrupted in the core - we'll handle that as a fatal error
//						errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), "Unsupported Action: " + actionId)));
						logger.error("Job " + backupJob.getJobId() + " faild with unsupported action: " + actionId);
					}
				} catch (ActionException e) {
					// Should only happen in case of problems in the backmeup-service (file I/O, DB access, etc.)
					// We'll handle that as a fatal error
//					errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), e.getMessage())));
					logger.error("Job " + backupJob.getJobId() + " faild with message: " + e);
				} finally {
					if (client != null) {
						client.close(); 
					}
				}
			}


			try {
				// Upload to Sink
//				addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.UPLOADING, StatusCategory.INFO, new Date().getTime()));
				logger.info("Job " + backupJob.getJobId() + " uploading");

				sinkProperties.setProperty("org.backmeup.tmpdir", getLastSplitElement(tmpDir, "/"));
				sinkProperties.setProperty("org.backmeup.userid", backupJob.getUser().getUserId() + "");
				sink.upload(sinkProperties, storage, new JobStatusProgressor(backupJob, "datasink"));

//				addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.SUCCESSFUL, StatusCategory.INFO, new Date().getTime()));
				logger.info("Job " + backupJob.getJobId() + " successful");
			} catch (StorageException e) {
//				logger.error("", e);
//				errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), e.getMessage())));
				logger.error("Job " + backupJob.getJobId() + " faild with message: " + e);
			}

			// store job protocol within database
//			storeJobProtocol(backupJob, protocol, storage.getDataObjectCount(), true);

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
			protocol.setSpace((int)currentSize);

			backupJob.addProtocol(protocol);
			backupJob.setJobStatus(JobStatus.successful);

		} catch (Exception e) {
//			logger.error("", e);
//			storeJobProtocol(backupJob, protocol, 0, false);
			
//			errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), e.getMessage())));
			logger.error("Job " + backupJob.getJobId() + " faild with message: " + e);	
			
			int processedItems = 0;
			try {
				processedItems = storage.getDataObjectCount();
			} catch (StorageException e1) {
				logger.error("", e);	
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
		/*
		if (!errorStatus.isEmpty()) {
			bmuService.sendEmail(job.getUser().getEmail(), MessageFormat.format(
					textBundle.getString(ERROR_EMAIL_SUBJECT), job.getUser().getEmail()),
					MessageFormat.format(
							textBundle.getString(ERROR_EMAIL_TEXT), job.getUser().getEmail(),
							job.getJobTitle()));
		}
		*/
		
		bmuService.updateBackupJob(backupJob);
	}

	private void doIndexing(Properties params, Storage storage, BackupJobDTO job, Client client) throws ActionException {
		// If we do indexing, the Thumbnail renderer needs to run before!
		Action thumbnailAction = ((PluginImpl)plugins).getAction("org.backmeup.thumbnail");
//		thumbnailAction.doAction(params, storage, job, new JobStatusProgressor(job, "thumbnailAction"));

		// After thumbnail rendering, run indexing
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", indexName).build();
		client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(indexHost, indexPort));

		Action indexAction = ((PluginImpl)plugins).getAction("org.backmeup.indexing");
//		indexAction.doAction(params, storage, job, new JobStatusProgressor(job,	"indexaction"));
		client.close();
	}
	
//	private JobStatus addStatusToDb(JobStatus status) {
//		logger.debug("Job status: {}", status.getMessage());
//		bmuService.saveStatus(status);
//		return status;
//	}

//	private void deleteOldStatus(BackupJobDTO persistentJob) {
//		bmuService.deleteStatusBefore(persistentJob.getJobId(), new Date());
//	}

//	private void storeJobProtocol(BackupJobDTO job, JobProtocolDTO protocol, int storedEntriesCount, boolean success) {
//		// remove old entries, then store the new one
//		bmuService.deleteJobProtocolByUsername(job.getUser().getUsername());
//
////		protocol.setUser(job.getUser());
////		protocol.setJobId(job.getJobId());
//		protocol.setSuccessful(success);
//		protocol.setProcessedItems(storedEntriesCount);
//
//		if (protocol.isSuccessful()) {
////			job.setLastSuccessful(protocol.getExecutionTime());
//			job.setJobStatus(JobStatus.successful);
//		} else {
////			job.setLastFailed(protocol.getExecutionTime());
//			job.setJobStatus(JobStatus.error);
//		}
//
//		bmuService.saveJobProtocol(job.getUser().getUsername(), job.getJobId(), protocol);
//	}

	private String generateTmpDirName(BackupJobDTO job, PluginProfileDTO profile) {
		SimpleDateFormat formatter = null;
		Date date = new Date();

		Long profileid = profile.getProfileId();
		Long jobid = job.getJobId();
		// Take only last part of "org.backmeup.xxxx" (xxxx)
		String profilename = getLastSplitElement(profile.getPluginId(), "\\.");

		formatter = new SimpleDateFormat(backupName.replaceAll("%PROFILEID%", profileid.toString()).replaceAll("%SOURCE%", profilename));

		return jobTempDir + "/" + jobid + "/" + formatter.format(date);
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

		private final BackupJobDTO job;
		private final String category;

		public JobStatusProgressor(BackupJobDTO job, String category) {
			this.job = job;
			this.category = category;
		}

		@Override
		public void progress(String message) {
//			addStatusToDb(new JobStatus(job.getJobId(), "info", category, new Date().getTime(),  message));
			logger.info("Job {} [{}] {}", job.getJobId(), category, message);
		}
	}
}
