package org.backmeup.job.impl;

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
import org.backmeup.model.BackupJob.JobStatus;
import org.backmeup.model.JobProtocol;
import org.backmeup.model.JobProtocol.JobProtocolMember;
import org.backmeup.model.ProfileOptions;
import org.backmeup.model.Status;
import org.backmeup.model.StatusCategory;
import org.backmeup.model.StatusType;
import org.backmeup.model.Token;
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
public class BackupJobRunner {

	private static final String ERROR_EMAIL_TEXT = "ERROR_EMAIL_TEXT";
	private static final String ERROR_EMAIL_SUBJECT = "ERROR_EMAIL_SUBJECT";
	private static final String ERROR_EMAIL_MIMETYPE = "ERROR_EMAIL_MIMETYPE";

	private static final String ES_CLUSTER_NAME = "es-backmeup-cluster";

	private String indexHost;

	private int indexPort;

	private String jobTempDir;

	private String backupName;

	private Plugin plugins;
	private KeyserverFacade keyserver;
	private BackmeupServiceFacade bmuService;

	private final Logger logger = LoggerFactory.getLogger(BackupJobRunner.class);

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

	private Status addStatusToDb(Status status) {
		logger.debug("STATUS: {}", status.getMessage());
		bmuService.saveStatus(status);
		return status;
	}

	private void deleteOldStatus(BackupJob persistentJob) {
		bmuService.deleteStatusBefore(persistentJob.getId(), new Date());
	}

	private void storeJobProtocol(BackupJob job, JobProtocol protocol, int storedEntriesCount, boolean success) {
		job = bmuService.findBackupJobById(job.getId());

		// remove old entries, then store the new one
		bmuService.deleteJobProtocolByUsername(job.getUser().getUsername());

		protocol.setUser(job.getUser());
		protocol.setJob(job);
		protocol.setSuccessful(success);
		protocol.setTotalStoredEntries(storedEntriesCount);

		if (protocol.isSuccessful()) {
			job.setLastSuccessful(protocol.getExecutionTime());
			job.setStatus(JobStatus.successful);
		} else {
			job.setLastFailed(protocol.getExecutionTime());
			job.setStatus(JobStatus.error);
		}

		bmuService.saveJobProtocol(protocol);
	}

	public void executeBackup(BackupJob job, Storage storage) {

		// use the job which is stored within the database
		BackupJob persistentJob = bmuService.findBackupJobById(job.getId());

		// when will the next access to the access data occur? current time
		// +
		// delay
		persistentJob.getToken().setBackupdate(new Date().getTime() + persistentJob.getDelay());

		// get access data + new token for next access
		AuthDataResult authenticationData = keyserver.getData(persistentJob.getToken());

		// the token for the next getData call
		Token newToken = authenticationData.getNewToken();
		persistentJob.setToken(newToken);
		persistentJob.setStatus(JobStatus.running);

		String userEmail = persistentJob.getUser().getEmail();
		String jobName = persistentJob.getJobTitle();

		// Store newToken for the next backup schedule
		bmuService.saveBackupJob(persistentJob);

		// Protocol Overview requires information about executed jobs
		JobProtocol protocol = new JobProtocol();
		Set<JobProtocolMember> protocolEntries = new HashSet<JobProtocolMember>();
		protocol.addMembers(protocolEntries);
		protocol.setSinkTitle(persistentJob.getSinkProfile().getProfileName());
		protocol.setExecutionTime(new Date());

		// track the error status messages
		List<Status> errorStatus = new ArrayList<Status>();

		// Open temporary storage
		try {
			Datasink sink = plugins.getDatasink(persistentJob.getSinkProfile().getDescription());
			Properties sinkProperties = authenticationData.getByProfileId(persistentJob.getSinkProfile().getProfileId());

			// delete previously stored status, as we only need the latest
			deleteOldStatus(persistentJob);
			addStatusToDb(new Status(persistentJob, "", StatusType.STARTED, StatusCategory.INFO, new Date()));
			long previousSize = 0;

			for (ProfileOptions po : persistentJob.getSourceProfiles()) {
				String tmpDir = generateTmpDirName(job, po);
				storage.open(tmpDir);

				Datasource source = plugins.getDatasource(po.getProfile().getDescription());

				Properties sourceProperties = authenticationData.getByProfileId(po.getProfile().getProfileId());
				List<String> sourceOptions = new ArrayList<String>();
				if (po.getOptions() != null) {
					sourceOptions.addAll(Arrays.asList(po.getOptions()));
				}

				addStatusToDb(new Status(persistentJob, "", StatusType.DOWNLOADING, StatusCategory.INFO, new Date()));

				// Download from source
				try {
					source.downloadAll(sourceProperties, sourceOptions, storage, new JobStatusProgressor(persistentJob, "datasource"));
				} catch (StorageException e) {
					errorStatus.add(addStatusToDb(new Status(persistentJob, e.getMessage(), StatusType.DOWNLOAD_FAILED,	StatusCategory.WARNING, new Date())));
				} catch (DatasourceException e) {
					errorStatus.add(addStatusToDb(new Status(persistentJob, e.getMessage(), StatusType.DOWNLOAD_FAILED, StatusCategory.WARNING, new Date())));
				}

				// for each datasource add an entry with bytes it consumed
				long currentSize = storage.getDataObjectSize() - previousSize;
				protocolEntries.add(new JobProtocolMember(protocol, po.getProfile().getProfileName(), currentSize));
				previousSize = storage.getDataObjectSize();

				// make properties global for the action loop. So the
				// plugins can communicate (filesplitt + encryption)
				Properties params = new Properties();
				params.putAll(sinkProperties);
				params.putAll(sourceProperties);

				// Execute Actions in sequence
				addStatusToDb(new Status(persistentJob, "",	StatusType.PROCESSING, StatusCategory.INFO, new Date()));

				// add all properties which have been stored to the params
				// collection
				for (ActionProfile actionProfile : persistentJob.getRequiredActions()) {
					for (ActionProperty ap : actionProfile.getActionOptions()) {
						params.put(ap.getKey(), ap.getValue());
					}
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
					addStatusToDb(new Status(persistentJob, "",	StatusType.UPLOADING, StatusCategory.INFO, new Date()));

					sinkProperties.setProperty("org.backmeup.tmpdir", getLastSplitElement(tmpDir, "/"));
					sinkProperties.setProperty("org.backmeup.userid", persistentJob.getUser().getUserId() + "");
					sink.upload(sinkProperties, storage, new JobStatusProgressor(persistentJob, "datasink"));
					addStatusToDb(new Status(persistentJob, "", StatusType.SUCCESSFUL, StatusCategory.INFO, new Date()));
				} catch (StorageException e) {
					errorStatus.add(addStatusToDb(new Status(persistentJob, e.getMessage(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date())));
				}

				// store job protocol within database
				storeJobProtocol(persistentJob, protocol, storage.getDataObjectCount(), true);

				storage.close();
			}
		} catch (StorageException e) {
			// job failed, store job protocol within database
			storeJobProtocol(persistentJob, protocol, 0, false);
			errorStatus.add(addStatusToDb(new Status(persistentJob, e.getMessage(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date())));
		} catch (Exception e) {
			storeJobProtocol(persistentJob, protocol, 0, false);
			errorStatus.add(addStatusToDb(new Status(persistentJob, e.getMessage(), StatusType.JOB_FAILED, StatusCategory.ERROR, new Date())));
		}
		// send error message, if there were any error status messages
		if (errorStatus.size() > 0) {
			bmuService.sendEmail(userEmail, MessageFormat.format(
					textBundle.getString(ERROR_EMAIL_SUBJECT), userEmail),
					MessageFormat.format(
							textBundle.getString(ERROR_EMAIL_TEXT), userEmail,
							jobName));
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

	private String generateTmpDirName(BackupJob job, ProfileOptions po) {
		SimpleDateFormat formatter = null;
		Date date = new Date();

		Long profileid = po.getProfile().getProfileId();
		Long jobid = job.getId();
		// Take only last part of "org.backmeup.xxxx" (xxxx)
		String profilename = getLastSplitElement(po.getProfile().getDescription(), "\\.");

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

		private BackupJob job;
		private String category;

		public JobStatusProgressor(BackupJob job, String category) {
			this.job = job;
			this.category = category;
		}

		@Override
		public void progress(String message) {
			addStatusToDb(new Status(job, message, "info", category, new Date()));
		}
	}
}
