package org.backmeup.worker;

import java.util.Queue;

import org.backmeup.job.impl.JobReceiver;
import org.backmeup.keyserver.client.KeyserverFacade;
import org.backmeup.keyserver.client.impl.KeyserverClient;
import org.backmeup.model.BackupJob;
import org.backmeup.plugin.Plugin;
import org.backmeup.plugin.osgi.PluginImpl;
import org.backmeup.service.client.BackmeupServiceFacade;
import org.backmeup.worker.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerCore {
	private final Logger logger = LoggerFactory.getLogger(WorkerCore.class);
	
	private int maxWorkerThreads;
	private int currentWorkerThreads;
	private Boolean initialized;
	private WorkerState currentState;
	
	private String pluginsDeploymentDir;
	private String pluginsTempDir;
	private String pluginsExportedPackages;
	private Plugin plugins;
	
	private JobReceiver jobReceiver;
	private Queue<BackupJob> jobQueue;
	
	
	
	// Getters and setters
	
	public WorkerState getCurrentState() {
		return currentState;
	}

	private void setCurrentState(WorkerState currentState) {
		this.currentState = currentState;
	}
	
	public int getNoOfCurrentJobs() {
		throw new UnsupportedOperationException();
	}

	public int getNoOfFinishedJobs() {
		throw new UnsupportedOperationException();
	}
	
	public int getNoOfFetchedJobs() {
		throw new UnsupportedOperationException();
	}
	
	public int getNoOfFailedJobs() {
		throw new UnsupportedOperationException();
	}
	
	// Constructor
	
	public WorkerCore() {
		pluginsDeploymentDir = Configuration.getProperty("backmeup.osgi.deploymentDirectory");
		pluginsTempDir = Configuration.getProperty("backmeup.osgi.temporaryDirectory");
		pluginsExportedPackages = Configuration.getProperty("backmeup.osgi.exportedPackages");
		plugins = new PluginImpl("deploydir", "tempDir", "expPack");
		
		maxWorkerThreads = Integer.parseInt(Configuration.getProperty("backmeup.worker.maxParallelJobs"));
		
		
		setCurrentState(WorkerState.Offline);
	}
	
	// Public Methods
	
	public void initialize() {
		logger.info("Initializing backmeup-worker");
		Boolean errorsDuringInit = false;
		
		plugins.startup();
		
		if(!errorsDuringInit) {
			setCurrentState(WorkerState.Idle);
		}
	}
	
	public void start() {
		
	}
	
	public void shutdown() {
		plugins.shutdown();
	}
	
	// Nested classes and enums
	
	private enum WorkerState {
		Offline, // Not connected to dependent services
		Idle,    // No jobs to execute
		Busy    // Jobs are currently running on worker
	}
}
