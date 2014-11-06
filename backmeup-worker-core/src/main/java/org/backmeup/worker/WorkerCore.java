package org.backmeup.worker;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.backmeup.model.exceptions.BackMeUpException;
import org.backmeup.plugin.Plugin;
import org.backmeup.worker.config.Configuration;
import org.backmeup.worker.job.BackupJobWorkerThread;
import org.backmeup.worker.job.receiver.JobReceivedEvent;
import org.backmeup.worker.job.receiver.JobReceivedListener;
import org.backmeup.worker.job.receiver.RabbitMQJobReceiver;
import org.backmeup.worker.job.threadpool.ObservableThreadPoolExecutor;
import org.backmeup.worker.job.threadpool.ThreadPoolListener;
import org.backmeup.worker.plugin.osgi.PluginImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerCore {
	private final Logger LOGGER = LoggerFactory.getLogger(WorkerCore.class);
	
	private Boolean initialized;
	private WorkerState currentState;
	
	private final int maxWorkerThreads;
	private final AtomicInteger noOfRunningJobs;
	private final AtomicInteger noOfFetchedJobs;
	private final AtomicInteger noOfFaildJobs;
	
	private final String pluginsDeploymentDir;
	private final String pluginsTempDir;
	private final String pluginsExportedPackages;
	private final Plugin plugins;
	
	private final String mqHost;
	private final String mqName;
	private final int    mqWaitInterval;
		
	private final String keyserverScheme;
	private final String keyserverHost;
	private final String keyserverPath;
	
	private final String bmuServiceScheme;
	private final String bmuServiceHost;
	private final String bmuServicePath;
	private final String bmuServiceAccessToken;
	
	private final String jobTempDir;
	private final String backupName;
	
	private final RabbitMQJobReceiver jobReceiver;
	private final BlockingQueue<Runnable> jobQueue;
	private final ObservableThreadPoolExecutor executorPool;
	
	// Constructor ------------------------------------------------------------
	
	public WorkerCore() {
		this.pluginsDeploymentDir = Configuration.getProperty("backmeup.osgi.deploymentDirectory");
		this.pluginsTempDir = Configuration.getProperty("backmeup.osgi.temporaryDirectory");
		this.pluginsExportedPackages = Configuration.getProperty("backmeup.osgi.exportedPackages");
		this.plugins = new PluginImpl(pluginsDeploymentDir, pluginsTempDir, pluginsExportedPackages);
		
		this.maxWorkerThreads = Integer.parseInt(Configuration.getProperty("backmeup.worker.maxParallelJobs"));
		
		this.noOfRunningJobs = new AtomicInteger(0);
		this.noOfFetchedJobs = new AtomicInteger(0);
		this.noOfFaildJobs = new AtomicInteger(0);
		
		this.mqHost = Configuration.getProperty("backmeup.message.queue.host");
		this.mqName = Configuration.getProperty("backmeup.message.queue.name");
		this.mqWaitInterval = 500;
				
		this.keyserverScheme = Configuration.getProperty("keyserver.scheme");
		this.keyserverHost = Configuration.getProperty("keyserver.host");
		this.keyserverPath = Configuration.getProperty("keyserver.path");
		
		this.bmuServiceScheme = Configuration.getProperty("backmeup.service.scheme");
		this.bmuServiceHost = Configuration.getProperty("backmeup.service.host");
		this.bmuServicePath = Configuration.getProperty("backmeup.service.path");
		this.bmuServiceAccessToken = Configuration.getProperty("backmeup.service.accessToken");
		
		this.jobTempDir = Configuration.getProperty("backmeup.job.temporaryDirectory");
		this.backupName = Configuration.getProperty("backmeup.job.backupname");
		
		this.jobReceiver = new RabbitMQJobReceiver(mqHost, mqName, mqWaitInterval);
		this.jobQueue = new ArrayBlockingQueue<>(maxWorkerThreads); // 2
		ThreadFactory threadFactory = Executors.defaultThreadFactory();
		this.executorPool = new ObservableThreadPoolExecutor(maxWorkerThreads, maxWorkerThreads, 10, TimeUnit.SECONDS, jobQueue, threadFactory);
		
		this.initialized = false;
		setCurrentState(WorkerState.Offline);
	}
	
	// Getters and setters ----------------------------------------------------
	
	public WorkerState getCurrentState() {
		return currentState;
	}

	private void setCurrentState(WorkerState currentState) {
		this.currentState = currentState;
	}
	
	public int getNoOfCurrentJobs() {
		return this.executorPool.getActiveCount();
	}
	
	public int getNoOfMaximumJobs() {
		return maxWorkerThreads;
	}

	public int getNoOfFinishedJobs() {
		return (int) this.executorPool.getCompletedTaskCount();
	}
	
	public int getNoOfFetchedJobs() {
		return noOfFetchedJobs.get();
	}
	
	public int getNoOfFailedJobs() {
		return noOfFaildJobs.get();
	}
	
	// Public Methods ---------------------------------------------------------
	
	public void initialize() {
		LOGGER.info("Initializing backmeup-worker");
		boolean errorsDuringInit = false;
		
		executorPool.addThreadPoolListener(new ThreadPoolListener() {
			@Override
			public void terminated() {				
			}
			
			@Override
			public void beforeExecute(Thread t, Runnable r) {
				jobThreadBeforeExecute(t, r);
			}
			
			@Override
			public void afterExecute(Runnable r, Throwable t) {
				jobThreadAterExecute(r, t);
			}
		});
		
		plugins.startup();
		
		jobReceiver.initialize();
		jobReceiver.addJobReceivedListener(new JobReceivedListener() {
			@Override
			public void jobReceived(JobReceivedEvent jre) {
				executeBackupJob(jre);
			}
		});
		
		if(!errorsDuringInit) {
			setCurrentState(WorkerState.Idle);
			initialized = true;
		}
	}
	
	public void start() {
		if(!initialized){
			throw new BackMeUpException("Worker not initialized");
		}
		
		jobReceiver.start();
		
	}
	
	public void shutdown() {
		executorPool.shutdown();
		plugins.shutdown();
		jobReceiver.stop();
	}
	
	// Private methods --------------------------------------------------------
	
	private void executeBackupJob(JobReceivedEvent jre) {
		noOfFetchedJobs.getAndIncrement();
		
		// if we reached our maximum concurrent job limit, pause the receiver
		if(getNoOfCurrentJobs() + 1 >= getNoOfMaximumJobs()) {
			jobReceiver.setPaused(true);
		}
		
		Long jobId = jre.getJobId();
		Runnable backupJobWorker = new BackupJobWorkerThread(
				jobId, plugins, 
				bmuServiceScheme, bmuServiceHost, bmuServicePath, bmuServiceAccessToken,
				keyserverScheme, keyserverHost, keyserverPath,
				jobTempDir, backupName);
		executorPool.execute(backupJobWorker);
	}
	
	private void jobThreadBeforeExecute(Thread t, Runnable r) {
		this.noOfRunningJobs.getAndIncrement();
		setCurrentState(WorkerState.Busy);
	}
	
	private void jobThreadAterExecute(Runnable r, Throwable t) {
		if(t != null) {
			noOfFaildJobs.getAndIncrement();
		}
		
		this.noOfRunningJobs.getAndDecrement();
		if (noOfRunningJobs.get() == 0) {
			setCurrentState(WorkerState.Idle);
		}
		jobReceiver.setPaused(false);
	}
	
	// Nested classes and enums -----------------------------------------------
	
	public enum WorkerState {
		Offline, // Not connected to dependent services
		Idle,    // No jobs to execute
		Busy    // Jobs are currently running on worker
	}
}
