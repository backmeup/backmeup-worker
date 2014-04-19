package org.backmeup.worker;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.backmeup.job.impl.BackupJobWorkerThread;
import org.backmeup.job.impl.JobReceivedEvent;
import org.backmeup.job.impl.JobReceivedListener;
import org.backmeup.job.impl.rabbitmq.RabbitMQJobReceiver;
import org.backmeup.model.BackupJob;
import org.backmeup.model.exceptions.BackMeUpException;
import org.backmeup.plugin.Plugin;
import org.backmeup.plugin.osgi.PluginImpl;
import org.backmeup.worker.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerCore {
	private final Logger logger = LoggerFactory.getLogger(WorkerCore.class);
	
	private Boolean initialized;
	private WorkerState currentState;
	
	private int maxWorkerThreads;
	private int currentWorkerThreads;
	private AtomicInteger noOfFetchedJobs;
	
	private final String pluginsDeploymentDir;
	private final String pluginsTempDir;
	private final String pluginsExportedPackages;
	private final Plugin plugins;
	
	private final String mqHost;
	private final String mqName;
	private final int    mqWaitInterval;
	
	private final String indexHost;
	private final int    indexPort;
	private final String indexClusterName;
	
	private final String jobTempDir;
	private final String backupName;
	
	private RabbitMQJobReceiver jobReceiver;
	private BlockingQueue<Runnable> jobQueue;
	private ThreadPoolExecutor executorPool;
	
	// Constructor ------------------------------------------------------------
	
	public WorkerCore() {
		this.pluginsDeploymentDir = Configuration.getProperty("backmeup.osgi.deploymentDirectory");
		this.pluginsTempDir = Configuration.getProperty("backmeup.osgi.temporaryDirectory");
		this.pluginsExportedPackages = Configuration.getProperty("backmeup.osgi.exportedPackages");
		this.plugins = new PluginImpl(pluginsDeploymentDir, pluginsTempDir, pluginsExportedPackages);
		
		this.maxWorkerThreads = Integer.parseInt(Configuration.getProperty("backmeup.worker.maxParallelJobs"));
		this.currentWorkerThreads = 0;
		
		this.noOfFetchedJobs = new AtomicInteger(0);
		
		this.mqHost = Configuration.getProperty("backmeup.message.queue.host");
		this.mqName = Configuration.getProperty("backmeup.message.queue.name");
		this.mqWaitInterval = 500;
		
		this.indexHost = Configuration.getProperty("backmeup.index.host");
		this.indexPort = Integer.parseInt(Configuration.getProperty("backmeup.index.port"));
		this.indexClusterName = Configuration.getProperty("backmeup.index.cluster.name");
		
		this.jobTempDir = Configuration.getProperty("backmeup.job.temporaryDirectory");
		this.backupName = Configuration.getProperty("backmeup.job.backupname");
		
		this.jobReceiver = new RabbitMQJobReceiver(mqHost, mqName, mqWaitInterval);
		this.jobQueue = new ArrayBlockingQueue<Runnable>(maxWorkerThreads); // 2
		ThreadFactory threadFactory = Executors.defaultThreadFactory();
		this.executorPool = new ThreadPoolExecutor(maxWorkerThreads, maxWorkerThreads, 10, TimeUnit.SECONDS, jobQueue, threadFactory);
		
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
		throw new UnsupportedOperationException();
	}

	public int getNoOfFinishedJobs() {
		throw new UnsupportedOperationException();
	}
	
	public int getNoOfFetchedJobs() {
		return noOfFetchedJobs.get();
	}
	
	public int getNoOfFailedJobs() {
		throw new UnsupportedOperationException();
	}
	
	// Public Methods ---------------------------------------------------------
	
	public void initialize() {
		logger.info("Initializing backmeup-worker");
		boolean errorsDuringInit = false;
		
		plugins.startup();
		jobReceiver.initialize();
		jobReceiver.addJobReceivedListener(new JobReceivedListener() {

			@Override
			public void jobReceived(JobReceivedEvent jre) {
				noOfFetchedJobs.getAndIncrement();
				
				BackupJob backupJob = jre.getBackupJob();
				Runnable backupJobWorker = new BackupJobWorkerThread(backupJob, plugins, indexHost, indexPort, jobTempDir, backupName);
				executorPool.execute(backupJobWorker);
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
	
	private void executeBackupJob(BackupJob backupJob) {
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub

			}
			
		});
	}
	
	// Nested classes and enums -----------------------------------------------
	
	private enum WorkerState {
		Offline, // Not connected to dependent services
		Idle,    // No jobs to execute
		Busy    // Jobs are currently running on worker
	}
}
