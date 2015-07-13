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

import org.backmeup.model.dto.WorkerConfigDTO;
import org.backmeup.model.dto.WorkerConfigDTO.DistributionMechanism;
import org.backmeup.model.dto.WorkerInfoDTO;
import org.backmeup.model.exceptions.BackMeUpException;
import org.backmeup.plugin.infrastructure.PluginManager;
import org.backmeup.service.client.BackmeupService;
import org.backmeup.service.client.impl.BackmeupServiceClient;
import org.backmeup.worker.config.Configuration;
import org.backmeup.worker.job.BackupJobWorkerThread;
import org.backmeup.worker.job.receiver.JobReceivedEvent;
import org.backmeup.worker.job.receiver.JobReceivedListener;
import org.backmeup.worker.job.receiver.RabbitMQJobReceiver;
import org.backmeup.worker.job.threadpool.ObservableThreadPoolExecutor;
import org.backmeup.worker.job.threadpool.ThreadPoolListener;
import org.backmeup.worker.perfmon.PerformanceMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.NumberGauge;

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
    
    @SuppressWarnings("unused")
    private final NumberGauge maxJobsGauge;
    @SuppressWarnings("unused")
    private final NumberGauge noOfRunningJobsGauge;

    private PluginManager pluginManager;
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
                this.workerName = this.workerId.toString();
            }
        }

        this.maxWorkerThreads = Integer.parseInt(Configuration.getProperty("backmeup.worker.maxParallelJobs"));
        this.maxJobsGauge = new NumberGauge(MonitorConfig.builder("maxJobs").build(), maxWorkerThreads);
        
        this.noOfRunningJobs = new AtomicInteger(0);
        this.noOfRunningJobsGauge = new NumberGauge(MonitorConfig.builder("runningJobs").build(), noOfRunningJobs);
        this.noOfFetchedJobs = new AtomicInteger(0);
        this.noOfFinishedJobs = new AtomicInteger(0);
        this.noOfFaildJobs = new AtomicInteger(0);

        String bmuServiceBaseUrl = Configuration.getProperty("backmeup.service.baseUrl");
        String bmuServiceAccessToken = Configuration.getProperty("backmeup.service.accessToken");
        this.bmuServiceClient = new BackmeupServiceClient(bmuServiceBaseUrl, bmuServiceAccessToken);

        this.jobTempDir = Configuration.getProperty("backmeup.worker.workDir");

        BlockingQueue<Runnable> jobQueue = new ArrayBlockingQueue<>(this.maxWorkerThreads);
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        this.executorPool = new ObservableThreadPoolExecutor(this.maxWorkerThreads, this.maxWorkerThreads, 10,
                TimeUnit.SECONDS, jobQueue, threadFactory);
        
        Monitors.registerObject(workerId.toString(), this);

        this.initialized = false;
        setCurrentState(WorkerState.OFFLINE);
    }

    // Getters and setters ----------------------------------------------------

    public WorkerState getCurrentState() {
        return this.currentState;
    }

    private void setCurrentState(WorkerState currentState) {
        this.currentState = currentState;
    }

    public int getNoOfCurrentJobs() {
        return this.executorPool.getActiveCount();
    }

    public int getNoOfMaximumJobs() {
        return this.maxWorkerThreads;
    }

    public int getNoOfFinishedJobs() {
        return this.noOfFinishedJobs.get();
    }

    public int getNoOfFetchedJobs() {
        return this.noOfFetchedJobs.get();
    }

    public int getNoOfFailedJobs() {
        return this.noOfFaildJobs.get();
    }

    // Public Methods ---------------------------------------------------------

    public void initialize() {
        LOGGER.info("Initializing backmeup-worker");
        boolean errorsDuringInit = false;

        WorkerInfoDTO workerInfo = getWorkerInfo();

        WorkerConfigDTO resp = null;
        try {
            //bmu service rest initialization might not yet have been properly completed at startup 
            resp = this.bmuServiceClient.initializeWorker(workerInfo);
            LOGGER.info("Initializing backmeup-worker - bmu service handshake done");
        } catch (BackMeUpException e) {
            LOGGER.info("Initializing backmeup-worker - bmu service handshake failed");
        }

        this.backupName = resp.getBackupNameTemplate();

        if (resp.getDistributionMechanism() == DistributionMechanism.QUEUE) {
            final StringTokenizer tokenizer = new StringTokenizer(resp.getConnectionInfo(), ";");
            final String mqHost = tokenizer.nextToken();
            final String mqName = tokenizer.nextToken();

            this.jobReceiver = new RabbitMQJobReceiver(mqHost, mqName, 500);
            this.jobReceiver.initialize();
            this.jobReceiver.addJobReceivedListener(new JobReceivedListener() {
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
        this.pluginManager = new PluginManager(pluginsDeploymentDir, pluginsTempDir, pluginsExportedPackages);
        this.pluginManager.startup();

        // Startup method should block until plugin infrastructure is
        // fully initialized and all plugins are loaded
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // Nothing to do
        }

        this.executorPool.addThreadPoolListener(new ThreadPoolListener() {
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
        
        PerformanceMonitor.initialize(bmuServiceClient);

        if (!errorsDuringInit) {
            setCurrentState(WorkerState.IDLE);
            this.initialized = true;
        }
    }

    public void start() {
        if (!this.initialized) {
            throw new WorkerException("Worker not initialized");
        }
        
        PerformanceMonitor.startPublishing();

        this.jobReceiver.start();
    }

    public void shutdown() {
        this.executorPool.shutdown();
        this.pluginManager.shutdown();
        this.jobReceiver.stop();
    }

    // Private methods --------------------------------------------------------

    private void executeBackupJob(JobReceivedEvent jre) {
        this.noOfFetchedJobs.getAndIncrement();

        // if we reached our maximum concurrent job limit, pause the receiver
        if (getNoOfCurrentJobs() + 1 >= getNoOfMaximumJobs()) {
            this.jobReceiver.setPaused(true);
        }

        Long jobId = jre.getJobId();
        Runnable backupJobWorker = new BackupJobWorkerThread(jobId, this.pluginManager, this.bmuServiceClient,
                this.jobTempDir, this.backupName);
        this.executorPool.execute(backupJobWorker);
    }

    private void jobThreadBeforeExecute(Thread t, Runnable r) {
        this.noOfRunningJobs.getAndIncrement();
        setCurrentState(WorkerState.BUSY);
    }

    private void jobThreadAterExecute(Runnable r, Throwable t) {
        if (t != null) {
            this.noOfFaildJobs.getAndIncrement();
        } else {
            this.noOfFinishedJobs.getAndIncrement();
        }

        this.noOfRunningJobs.getAndDecrement();
        if (this.noOfRunningJobs.get() == 0) {
            setCurrentState(WorkerState.IDLE);
        }
        this.jobReceiver.setPaused(false);
    }

    private WorkerInfoDTO getWorkerInfo() {
        WorkerInfoDTO workerInfo = new WorkerInfoDTO();

        workerInfo.setWorkerId(this.workerId);
        workerInfo.setWorkerName(this.workerName);
        workerInfo.setOsName(System.getProperty("os.name"));
        workerInfo.setOsVersion(System.getProperty("os.version"));
        workerInfo.setOsArchitecture(System.getProperty("os.arch"));
        workerInfo.setTotalMemory(Runtime.getRuntime().totalMemory());
        workerInfo.setTotalCPUCores(Runtime.getRuntime().availableProcessors());
        long totalSpace = new File(this.jobTempDir).getTotalSpace();
        workerInfo.setTotalSpace(totalSpace);

        return workerInfo;
    }

    // Nested classes and enums -----------------------------------------------

    public enum WorkerState {
        // Not connected to dependent services
        OFFLINE,

        // No jobs to execute
        IDLE,

        // Jobs are currently running on worker
        BUSY
    }
}
