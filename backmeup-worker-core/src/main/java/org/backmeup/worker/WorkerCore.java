package org.backmeup.worker;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.backmeup.keyserver.client.KeyserverClient;
import org.backmeup.model.dto.WorkerConfigDTO;
import org.backmeup.model.dto.WorkerConfigDTO.DistributionMechanism;
import org.backmeup.model.dto.WorkerInfoDTO;
import org.backmeup.plugin.Plugin;
import org.backmeup.service.client.BackmeupService;
import org.backmeup.service.client.impl.BackmeupServiceClient;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerCore.class);
    
    private final UUID workerId;
    private String workerName;

    private Boolean initialized;
    private WorkerState currentState;

    private final int maxWorkerThreads;
    private final AtomicInteger noOfRunningJobs;
    private final AtomicInteger noOfFetchedJobs;
    private final AtomicInteger noOfFinishedJobs;
    private final AtomicInteger noOfFaildJobs;

    private Plugin plugins;
    private final KeyserverClient keyserverClient;
    private final BackmeupService bmuServiceClient;

    private final String jobTempDir;
    private String backupName;

    private RabbitMQJobReceiver jobReceiver;
    private final ObservableThreadPoolExecutor executorPool;

    // Constructor ------------------------------------------------------------

    public WorkerCore() {
        String wId = Configuration.getProperty("backmeup.worker.id");
        if (wId != null) {
            this.workerId = UUID.fromString(wId);
        } else {
            this.workerId = UUID.randomUUID();
        }
        
        String wName = Configuration.getProperty("backmeup.worker.name");
        if (wName != null) {
            this.workerName = wName;
        } else {
            try {
                this.workerName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                LOGGER.error("", e);
                this.workerName = workerId.toString();
            }
        }
        
        this.maxWorkerThreads = Integer.parseInt(Configuration.getProperty("backmeup.worker.maxParallelJobs"));

        this.noOfRunningJobs = new AtomicInteger(0);
        this.noOfFetchedJobs = new AtomicInteger(0);
        this.noOfFinishedJobs = new AtomicInteger(0);
        this.noOfFaildJobs = new AtomicInteger(0);

        String keyserverBaseUrl = Configuration.getProperty("backmeup.keyserver.baseUrl");
        String workerAppId = Configuration.getProperty("backmeup.worker.appId");
        String workerAppSecret = Configuration.getProperty("backmeup.worker.appSecret");
        this.keyserverClient = new KeyserverClient(keyserverBaseUrl, workerAppId, workerAppSecret);
        

        String bmuServiceScheme = Configuration.getProperty("backmeup.service.scheme");
        String bmuServiceHost = Configuration.getProperty("backmeup.service.host");
        String bmuServicePath = Configuration.getProperty("backmeup.service.path");
        String bmuServiceAccessToken = Configuration.getProperty("backmeup.service.accessToken");
        this.bmuServiceClient = new BackmeupServiceClient(bmuServiceScheme, bmuServiceHost, bmuServicePath, bmuServiceAccessToken);

        this.jobTempDir = Configuration.getProperty("backmeup.worker.workDir");

        BlockingQueue<Runnable> jobQueue = new ArrayBlockingQueue<>(maxWorkerThreads);
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        this.executorPool = new ObservableThreadPoolExecutor(maxWorkerThreads, maxWorkerThreads, 10, TimeUnit.SECONDS, jobQueue, threadFactory);

        this.initialized = false;
        setCurrentState(WorkerState.OFFLINE);
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
        return noOfFinishedJobs.get();
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
        
        WorkerInfoDTO workerInfo = getWorkerInfo();
        WorkerConfigDTO resp = bmuServiceClient.initializeWorker(workerInfo);
        
        this.backupName = resp.getBackupNameTemplate();
        
        if (resp.getDistributionMechanism() == DistributionMechanism.QUEUE) {
            final StringTokenizer tokenizer = new StringTokenizer(resp.getConnectionInfo(), ";");
            final String mqHost = tokenizer.nextToken();
            final String mqName = tokenizer.nextToken();
            
            this.jobReceiver = new RabbitMQJobReceiver(mqHost, mqName, 500);
            jobReceiver.initialize();
            jobReceiver.addJobReceivedListener(new JobReceivedListener() {
                @Override
                public void jobReceived(JobReceivedEvent jre) {
                    executeBackupJob(jre);
                }
            });
        } else {
            // DistributionMechanism not supported
            errorsDuringInit = true;
        }
        
        // Initialize plugins infrastructure and load all plugins
        String pluginsDeploymentDir = Configuration.getProperty("backmeup.osgi.deploymentDirectory");
        String pluginsTempDir = Configuration.getProperty("backmeup.osgi.temporaryDirectory");
        String pluginsExportedPackages = resp.getPluginsExportedPackages();
        this.plugins = new PluginImpl(pluginsDeploymentDir, pluginsTempDir, pluginsExportedPackages);
        plugins.startup();
        
        // TODO: Fix workaround. Startup method should block until plugin
        // infrastructure is fully initialized and all plugins are loaded
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // Nothing to do
        }
        
        executorPool.addThreadPoolListener(new ThreadPoolListener() {
            @Override
            public void terminated() {
                // Nothing to do here
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
        
        if(!errorsDuringInit) {
            setCurrentState(WorkerState.IDLE);
            initialized = true;
        }
    }

    public void start() {
        if(!initialized){
            throw new WorkerException("Worker not initialized");
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
        Runnable backupJobWorker = new BackupJobWorkerThread(jobId, plugins, bmuServiceClient, keyserverClient, jobTempDir, backupName);
        executorPool.execute(backupJobWorker);   
    }

    private void jobThreadBeforeExecute(Thread t, Runnable r) {
        this.noOfRunningJobs.getAndIncrement();
        setCurrentState(WorkerState.BUSY);
    }

    private void jobThreadAterExecute(Runnable r, Throwable t) {
        if(t != null) {
            noOfFaildJobs.getAndIncrement();
        } else {
            noOfFinishedJobs.getAndIncrement();
        }

        this.noOfRunningJobs.getAndDecrement();
        if (noOfRunningJobs.get() == 0) {
            setCurrentState(WorkerState.IDLE);
        }
        jobReceiver.setPaused(false);
    }
    
    private WorkerInfoDTO getWorkerInfo() {
        WorkerInfoDTO workerInfo = new WorkerInfoDTO();
        
        workerInfo.setWorkerId(workerId);
        workerInfo.setWorkerName(workerName);
        workerInfo.setOsName(System.getProperty("os.name"));
        workerInfo.setOsVersion(System.getProperty("os.version"));
        workerInfo.setOsArchitecture(System.getProperty("os.arch"));
        workerInfo.setTotalMemory(Runtime.getRuntime().totalMemory());
        workerInfo.setTotalCPUCores(Runtime.getRuntime().availableProcessors());
        long totalSpace = new File(jobTempDir).getTotalSpace();
        workerInfo.setTotalSpace(totalSpace);
        
        return workerInfo;
    }

    // Nested classes and enums -----------------------------------------------

    public enum WorkerState {
        OFFLINE, // Not connected to dependent services
        IDLE,    // No jobs to execute
        BUSY     // Jobs are currently running on worker
    }
}
