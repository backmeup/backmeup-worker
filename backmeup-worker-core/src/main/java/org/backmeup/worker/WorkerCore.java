package org.backmeup.worker;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.StringTokenizer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.backmeup.model.dto.WorkerConfigDTO;
import org.backmeup.model.dto.WorkerConfigDTO.DistributionMechanism;
import org.backmeup.model.dto.WorkerInfoDTO;
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
import org.backmeup.worker.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.NumberGauge;

public class WorkerCore {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerCore.class);
    private static final int WORKER_CONFIG_TIMEOUT_SECONDS = 60;

    private final String workerId;
    private final String workerSecret;
    private String workerName;

    private Boolean initialized;
    private WorkerState currentState;

    private final int maxWorkerThreads;
    private final AtomicInteger noOfRunningJobs;
    private final AtomicInteger noOfFetchedJobs;
    private final AtomicInteger noOfFinishedJobs;
    private final AtomicInteger noOfFaildJobs;

    @SuppressWarnings({ "unused", "PMD.SingularField" })
    private final NumberGauge maxJobsGauge;
    @SuppressWarnings({ "unused", "PMD.SingularField" })
    private final NumberGauge noOfRunningJobsGauge;

    private PluginManager pluginManager;
    private final BackmeupService bmuServiceClient;

    private final String jobTempDir;
    private String backupName;

    private RabbitMQJobReceiver jobReceiver;
    private final ObservableThreadPoolExecutor executorPool;

    // Constructor ------------------------------------------------------------

    public WorkerCore() {
        this.workerId = Configuration.getProperty("backmeup.worker.appId");
        if (StringUtils.isEmpty(this.workerId)) {
            throw new WorkerException("Worker id is not set");
        }

        this.workerSecret = Configuration.getProperty("backmeup.worker.appSecret");
        if (StringUtils.isEmpty(this.workerSecret)) {
            throw new WorkerException("Worker secret is not set");
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
        this.maxJobsGauge = new NumberGauge(MonitorConfig.builder("maxJobs").build(), this.maxWorkerThreads);

        this.noOfRunningJobs = new AtomicInteger(0);
        this.noOfRunningJobsGauge = new NumberGauge(MonitorConfig.builder("runningJobs").build(), this.noOfRunningJobs);
        this.noOfFetchedJobs = new AtomicInteger(0);
        this.noOfFinishedJobs = new AtomicInteger(0);
        this.noOfFaildJobs = new AtomicInteger(0);

        String bmuServiceBaseUrl = Configuration.getProperty("backmeup.service.baseUrl");
        this.bmuServiceClient = new BackmeupServiceClient(bmuServiceBaseUrl);

        this.jobTempDir = Configuration.getProperty("backmeup.worker.workDir");

        BlockingQueue<Runnable> jobQueue = new ArrayBlockingQueue<>(this.maxWorkerThreads);
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        this.executorPool = new ObservableThreadPoolExecutor(this.maxWorkerThreads, this.maxWorkerThreads, 10, TimeUnit.SECONDS, jobQueue,
                threadFactory);

        Monitors.registerObject(this.workerId.toString(), this);

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
        LOGGER.info("Authenticate backmeup-worker");
        this.bmuServiceClient.authenticateWorker(this.workerId, this.workerSecret);

        LOGGER.info("Initializing backmeup-worker");
        boolean errorsDuringInit = false;

        WorkerInfoDTO workerInfo = getWorkerInfo();
        WorkerConfigDTO resp = getWorkerConfig(workerInfo, WORKER_CONFIG_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        try {
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

        } catch (Exception e) {
            LOGGER.error("Error setting worker configuraiton", e);
            errorsDuringInit = true;
        }

        try {
            // Initialize plugins infrastructure and load all plugins
            String pluginsDeploymentDir = Configuration.getProperty("backmeup.osgi.deploymentDirectory");
            String pluginsTempDir = Configuration.getProperty("backmeup.osgi.temporaryDirectory");
            String pluginsExportedPackages = resp.getPluginsExportedPackages();
            this.pluginManager = new PluginManager(pluginsDeploymentDir, pluginsTempDir, pluginsExportedPackages);
            this.pluginManager.startup();
        } catch (Exception e) {
            LOGGER.error("Error initializing plugin infrastructure", e);
            errorsDuringInit = true;
        }

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

        if (Boolean.parseBoolean(Configuration.getProperty("backmeup.worker.publishMetrics", "false"))) {
            PerformanceMonitor.initialize(this.bmuServiceClient);
        } else {
            PerformanceMonitor.initialize();
        }

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
        Runnable backupJobWorker = new BackupJobWorkerThread(jobId, this.pluginManager, this.bmuServiceClient, this.jobTempDir,
                this.backupName);
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
        final WorkerInfoDTO workerInfo = new WorkerInfoDTO();

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

    private WorkerConfigDTO getWorkerConfig(WorkerInfoDTO workerInfo, long timeout, TimeUnit timeUnit) {
        LOGGER.info("Obtaining worker config");

        int retries = 0;
        final int sleepTime = 1000;
        final long startTime = System.currentTimeMillis();
        final long abortTime = timeUnit.toMillis(timeout);
        WorkerConfigDTO config = null;

        while ((config == null) && ((System.currentTimeMillis() - startTime) < abortTime)) {
            try {
                if (retries != 0) {
                    LOGGER.info(String.format("Obtaining worker config failed, retrying (#%d)...", retries));
                }
                config = this.bmuServiceClient.initializeWorker(workerInfo);
            } catch (Exception e) {
                LOGGER.error("", e);
            } finally {
                retries++;
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    LOGGER.error("", e);
                }
            }
        }

        if (config != null) {
            LOGGER.info("Obtaining worker config successful");
            return config;
        } else {
            LOGGER.info("Obtaining worker config failed");
            throw new WorkerException("Failed obtaining worker configuration");
        }
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
