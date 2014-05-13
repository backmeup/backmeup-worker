package org.backmeup.worker.job;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;

import org.backmeup.keyserver.client.KeyserverFacade;
import org.backmeup.keyserver.model.AuthDataResult;
import org.backmeup.model.ActionProfile;
import org.backmeup.model.ActionProfile.ActionProperty;
import org.backmeup.model.BackupJob;
import org.backmeup.model.JobProtocol;
import org.backmeup.model.JobProtocol.JobProtocolMember;
import org.backmeup.model.ProfileOptions;
import org.backmeup.model.StatusCategory;
import org.backmeup.model.StatusType;
import org.backmeup.model.Token;
import org.backmeup.model.constants.BackupJobStatus;
import org.backmeup.model.dto.ActionProfileDTO;
import org.backmeup.model.dto.DatasourceProfile;
import org.backmeup.model.dto.Job;
import org.backmeup.model.dto.JobProtocolDTO;
import org.backmeup.model.dto.JobProtocolMemberDTO;
import org.backmeup.model.dto.JobStatus;
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

	private String indexHost;
	private int    indexPort;
	private String indexName = "es-backmeup-cluster";

	private String jobTempDir;
	private String backupName;

	private Plugin plugins;
	private KeyserverFacade keyserver;
	private BackmeupServiceFacade bmuService;

	private ResourceBundle textBundle = ResourceBundle.getBundle("BackupJobRunner");

	public BackupJobRunner(Plugin plugins, KeyserverFacade keyserver, BackmeupServiceFacade bmuService, String indexHost, int indexPort, String jobTempDir, String backupName) {
		this.plugins = plugins;
		this.keyserver = keyserver;
		this.bmuService = bmuService;
		this.indexHost = indexHost;
		this.indexPort = indexPort;
		this.jobTempDir = jobTempDir;
		this.backupName = backupName;
	}

	public void executeBackup(BackupJob job, Storage storage) {

		// use the job which is stored within the database
		Job persistentJob = bmuService.findBackupJobById(job.getUser().getUsername(), job.getId());
		// when will the next access to the access data occur? current time + delay
		persistentJob.setBackupDate(new Date().getTime() + persistentJob.getDelay());

		// get access data + new token for next access
		Token token = new Token(
				persistentJob.getToken(),
				persistentJob.getTokenId(),
				persistentJob.getBackupDate());
		AuthDataResult authenticationData = keyserver.getData(token);

		// the token for the next getData call
		Token newToken = authenticationData.getNewToken();
		
		// Set the new token information in the current job
		persistentJob.setTokenId(newToken.getTokenId());
		persistentJob.setToken(newToken.getToken());
		persistentJob.setBackupDate(newToken.getBackupdate());
		
		persistentJob.setStatus(BackupJobStatus.running);

		// Store newToken for the next backup schedule
		// Only store token?
		// Set job status 'running'
//		bmuService.saveBackupJob(persistentJob);

		// Protocol Overview requires information about executed jobs
		JobProtocolDTO protocol = new JobProtocolDTO();
//		Set<JobProtocolMemberDTO> protocolEntries = new HashSet<JobProtocolMemberDTO>();
//		protocol.addMembers(protocolEntries);
		protocol.setSinkTitle(persistentJob.getDatasink().getProfileName());
		protocol.setExecutionTime(new Date().getTime());

		// track the error status messages
		List<JobStatus> errorStatus = new ArrayList<JobStatus>();

		// Open temporary storage
		try {
			Datasink sink = plugins.getDatasink(persistentJob.getDatasink().getDescription());
			Properties sinkProperties = authenticationData.getByProfileId(persistentJob.getDatasink().getProfileId());

			// delete previously stored status, as we only need the latest
			deleteOldStatus(persistentJob);
			addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.STARTED, StatusCategory.INFO, new Date().getTime()));
			long previousSize = 0;

			for (DatasourceProfile sourceProfile : persistentJob.getDatasources()) {
				String tmpDir = generateTmpDirName(persistentJob, sourceProfile);
				storage.open(tmpDir);

				Datasource source = plugins.getDatasource(sourceProfile.getDescription());

//				Properties sourceProperties = authenticationData.getByProfileId(po.getProfile().getProfileId());
				Properties sourceProperties = authenticationData.getByProfileId(sourceProfile.getDatasourceId());
				
				List<String> sourceOptions = new ArrayList<String>();
				if (sourceProfile.getDatasourceOptions() != null) {
					sourceOptions.addAll(sourceProfile.getDatasourceOptions());
				}

				addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.DOWNLOADING, StatusCategory.INFO, new Date().getTime()));

				// Download from source
				try {
					source.downloadAll(sourceProperties, sourceOptions, storage, new JobStatusProgressor(persistentJob, "datasource"));
				} catch (StorageException e) {
					logger.error("", e);
					errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.DOWNLOAD_FAILED, StatusCategory.WARNING, new Date().getTime(), e.getMessage())));
				} catch (DatasourceException e) {
					logger.error("", e);
					errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.DOWNLOAD_FAILED, StatusCategory.WARNING, new Date().getTime(), e.getMessage())));
				}

				// for each datasource add an entry with bytes it consumed
				long currentSize = storage.getDataObjectSize() - previousSize;
				protocol.addMember(new JobProtocolMemberDTO(protocol.getId(), "po.getProfile().getProfileName()", currentSize));
				previousSize = storage.getDataObjectSize();

				// make properties global for the action loop. So the
				// plugins can communicate (filesplitt + encryption)
				Properties params = new Properties();
				params.putAll(sinkProperties);
				params.putAll(sourceProperties);

				// Execute Actions in sequence
				addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.PROCESSING, StatusCategory.INFO, new Date().getTime()));

				// add all properties which have been stored to the params
				// collection
				for (ActionProfileDTO actionProfile : persistentJob.getActions()) {
					params.putAll(actionProfile.getOptions());
				}

				/*
				
				// Run indexing in case the user has enabled it using the
				// 'enable.indexing' user property
				boolean doIndexing = true; // We're using true as the
											// default value for now

				String enableIndexing = persistentJob.getUser().getUserProperty("enable.indexing");
				if (enableIndexing != null) {
					if (enableIndexing.toLowerCase().trim().equals("false")) {
						doIndexing = false;
					}
				}

				// has the indexer been requested during creation of the
				// backup job?
				ActionDescribable ad = new IndexDescribable();
				List<ActionProfile> aps = persistentJob.getRequiredActions();
				ActionProfile indexer = null;
				for (ActionProfile ap : aps) {
					if ("org.backmeup.indexer".equals(ap.getActionId())) {
						indexer = ap;
						break;
					}
				}

				if (doIndexing && indexer == null) {
					// if we need to index, add the indexer to the requested actions
					aps.add(new ActionProfile(ad.getId(), ad.getPriority()));
				}

				for (ActionProfile actionProfile : persistentJob.getSortedRequiredActions()) {
					String actionId = actionProfile.getActionId();
					Client client = null;
					
					try {
						if ("org.backmeup.filesplitting".equals(actionId)) {
							// If we do encryption, the Filesplitter needs to run before!
							Action filesplitAction = new FilesplittAction();
							filesplitAction.doAction(params, storage, persistentJob, new JobStatusProgressor(persistentJob, "filesplittaction"));
						} else if ("org.backmeup.encryption".equals(actionId)) {
							// Add the encryption password to the parameters
							if (authenticationData.getEncryptionPwd() != null) {
								params.put("org.backmeup.encryption.password", authenticationData.getEncryptionPwd());
							}

							// After splitting, run encryption
							Action encryptionAction = new EncryptionAction();
							encryptionAction.doAction(params, storage, job,	new JobStatusProgressor(persistentJob, "encryptionaction"));
						} else if ("org.backmeup.indexer".equals(actionId)) {
							// Do nothing - we ignore index action declaration in the job description and use
							// the info from the user properties instead
							if (doIndexing) {
								doIndexing(params, storage, persistentJob, client);
							}

						} else {
							// Only happens in case Job was corrupted in the
							// core - we'll handle that as a fatal error
							errorStatus.add(addStatusToDb(new Status(persistentJob, "Unsupported Action: " + actionId, StatusType.JOB_FAILED, StatusCategory.ERROR, new Date())));
						}
					} catch (ActionException e) {
						// Should only happen in case of problems in the
						// core (file I/O, DB access, etc.) - we'll handle
						// that as a fatal error
						errorStatus.add(addStatusToDb(new Status(persistentJob, e.getMessage(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date())));
					} finally {
						if (client != null) {
							client.close(); 
						}
					}
				}
				
				*/

				try {
					// Upload to Sink
					addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.UPLOADING, StatusCategory.INFO, new Date().getTime()));

					sinkProperties.setProperty("org.backmeup.tmpdir", getLastSplitElement(tmpDir, "/"));
					sinkProperties.setProperty("org.backmeup.userid", persistentJob.getUser().getUserId() + "");
					sink.upload(sinkProperties, storage, new JobStatusProgressor(persistentJob, "datasink"));
					addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.SUCCESSFUL, StatusCategory.INFO, new Date().getTime()));
				} catch (StorageException e) {
					logger.error("", e);
					errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), e.getMessage())));
				}

				// store job protocol within database
				storeJobProtocol(persistentJob, protocol, storage.getDataObjectCount(), true);

				storage.close();
			}
		} catch (StorageException e) {
			logger.error("", e);
			// job failed, store job protocol within database
			storeJobProtocol(persistentJob, protocol, 0, false);
			errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), e.getMessage())));
		} catch (Exception e) {
			logger.error("", e);
			storeJobProtocol(persistentJob, protocol, 0, false);
			errorStatus.add(addStatusToDb(new JobStatus(persistentJob.getJobId(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date().getTime(), e.getMessage())));
		}
		// send error message, if there were any error status messages
		if (!errorStatus.isEmpty()) {
			bmuService.sendEmail(job.getUser().getEmail(), MessageFormat.format(
					textBundle.getString(ERROR_EMAIL_SUBJECT), job.getUser().getEmail()),
					MessageFormat.format(
							textBundle.getString(ERROR_EMAIL_TEXT), job.getUser().getEmail(),
							job.getJobTitle()));
		}
	}

	/*
	private void doIndexing(Properties params, Storage storage, BackupJob job,
			Client client) throws ActionException {
		// If we do indexing, the Thumbnail renderer needs to run before!
		Action thumbnailAction = new ThumbnailAction();
		thumbnailAction.doAction(params, storage, job, new JobStatusProgressor(job, "thumbnailAction"));

		// After thumbnail rendering, run indexing
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", ES_CLUSTER_NAME).build();
		client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(indexHost, indexPort));

		Action indexAction = new IndexAction(client);
		indexAction.doAction(params, storage, job, new JobStatusProgressor(job,	"indexaction"));
		client.close();
	}
	*/
	
	private JobStatus addStatusToDb(JobStatus status) {
		logger.debug("Job status: {}", status.getMessage());
		bmuService.saveStatus(status);
		return status;
	}

	private void deleteOldStatus(Job persistentJob) {
		bmuService.deleteStatusBefore(persistentJob.getJobId(), new Date());
	}

	private void storeJobProtocol(Job job, JobProtocolDTO protocol, int storedEntriesCount, boolean success) {
		// remove old entries, then store the new one
		bmuService.deleteJobProtocolByUsername(job.getUser().getUsername());

		protocol.setUser(job.getUser());
		protocol.setJobId(job.getJobId());
		protocol.setSuccessful(success);
		protocol.setTotalStoredEntries(storedEntriesCount);

		if (protocol.isSuccessful()) {
			job.setLastSuccessful(protocol.getExecutionTime());
			job.setStatus(BackupJobStatus.successful);
		} else {
			job.setLastFailed(protocol.getExecutionTime());
			job.setStatus(BackupJobStatus.error);
		}

		bmuService.saveJobProtocol(job.getUser().getUsername(), job.getJobId(), protocol);
	}

	private String generateTmpDirName(Job job, DatasourceProfile profile) {
		SimpleDateFormat formatter = null;
		Date date = new Date();

		Long profileid = profile.getDatasourceId();
		Long jobid = job.getJobId();
		// Take only last part of "org.backmeup.xxxx" (xxxx)
		String profilename = getLastSplitElement(profile.getDescription(), "\\.");

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

		private Job job;
		private String category;

		public JobStatusProgressor(Job job, String category) {
			this.job = job;
			this.category = category;
		}

		@Override
		public void progress(String message) {
			addStatusToDb(new JobStatus(job.getJobId(), "info", category, new Date().getTime(),  message));
		}
	}
}
