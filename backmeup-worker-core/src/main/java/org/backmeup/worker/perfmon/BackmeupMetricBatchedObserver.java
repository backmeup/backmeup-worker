package org.backmeup.worker.perfmon;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.backmeup.model.dto.WorkerMetricDTO;
import org.backmeup.service.client.BackmeupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.Metric;
import com.netflix.servo.publish.MetricObserver;

@SuppressWarnings("unused")
public class BackmeupMetricBatchedObserver implements MetricObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackmeupMetricBatchedObserver.class);

    private static final int BATCH_SIZE_DEFAULT = 1000;
    private static final String METRIC_PREFIX_DEFAULT = "backmeup.worker";

    private final String metricPrefix;
    private final int pushQueueSize = 1000;
    private final int batchSize;
    private final long sendTimeoutMs = 1000;
    private final BackmeupService backmeupServiceClient;

    private final BlockingQueue<UpdateTask> pushQueue = new LinkedBlockingQueue<UpdateTask>(pushQueueSize);

    // Whether we should send metrics. May be used when running in dev environment.
    private volatile boolean sendMetrics = true;

    public BackmeupMetricBatchedObserver(BackmeupService backmeupServiceClient) {
        this(backmeupServiceClient, METRIC_PREFIX_DEFAULT, BATCH_SIZE_DEFAULT);
    }

    public BackmeupMetricBatchedObserver(BackmeupService backmeupServiceClient, String metricPrefix) {
        this(backmeupServiceClient, metricPrefix, BATCH_SIZE_DEFAULT);
    }

    public BackmeupMetricBatchedObserver(BackmeupService backmeupServiceClient, String metricPrefix, int batchSize) {
        this.backmeupServiceClient = backmeupServiceClient;
        this.metricPrefix = metricPrefix;
        this.batchSize = batchSize;

        final Thread pushThread = new Thread(new PushProcessor(), "BackmeupMetricObserver-Push");
        pushThread.setDaemon(true);
        pushThread.start();
    }

    @Override
    public void update(List<Metric> metrics) {
        if (!sendMetrics) {
            LOGGER.debug("BackmeupMetricObserver is disabled. Not sending metrics.");
            return;
        }

        if (metrics.isEmpty()) {
            LOGGER.debug("List of metrics is empty. No metrics to send.");
            return;
        }

        final int numMetrics = metrics.size();
        final Metric[] atlasMetrics = new Metric[metrics.size()];
        metrics.toArray(atlasMetrics);
        LOGGER.debug("Writing {} metrics to backmeup-service ({})", numMetrics, "http://localhost...");

        int i = 0;
        while (i < numMetrics) {
            final int remaining = numMetrics - i;
            final int batchSize = Math.min(remaining, this.batchSize);
            final Metric[] batch = new Metric[batchSize];
            System.arraycopy(atlasMetrics, i, batch, 0, batchSize);
            
            UpdateTask task = new UpdateTask(batchSize, batch);
            sendToQueue(task);
            
            i += batchSize;
        }
    }

    @Override
    public String getName() {
        return "backmeup-service";
    }
    
    private void sendNow(UpdateTask updateTasks) {
        if (updateTasks.numMetrics == 0) {
            return;
        }

        int totalSent = 0;
        try {
            List<WorkerMetricDTO> workerMetrics = new ArrayList<>(updateTasks.metrics.length);
            for(Metric m : updateTasks.metrics) {
                WorkerMetricDTO metric = new WorkerMetricDTO();
                metric.setTimestamp(new Date(m.getTimestamp()));
                metric.setMetric( m.getConfig().getName());
                metric.setValue(m.getNumberValue().doubleValue());
                workerMetrics.add(metric);
            }
            this.backmeupServiceClient.addWorkerMetrics(workerMetrics);
            LOGGER.debug("Sent {}/{} metrics to backmeup-service", totalSent, updateTasks.numMetrics);
        } finally {
        }
    }
    
    private void sendToQueue(UpdateTask task) {
        final int maxAttempts = 5;
        int attempts = 1;
        while (!pushQueue.offer(task) && attempts <= maxAttempts) {
            attempts++;
            final UpdateTask droppedTask = pushQueue.remove();
            LOGGER.warn(
                    "Remove old task because queue is full. Dropping {} metrics.",
                    droppedTask.numMetrics);
        }
        if (attempts >= maxAttempts) {
            LOGGER.error("Unable to push update of {}", task);
        } else {
            LOGGER.debug("Queued push of {}", task);
        }
    }

    private static class UpdateTask {
        private final int numMetrics;
        private final Metric[] metrics;

        UpdateTask(int numMetrics, Metric[] metrics) {
            this.numMetrics = numMetrics;
            this.metrics = metrics;
        }

        @Override
        public String toString() {
            return "UpdateTasks{numMetrics=" + numMetrics + '}';
        }
    }

    private class PushProcessor implements Runnable {
        @Override
        public void run() {
            boolean interrupted = false;
            while (!interrupted) {
                try {
                    sendNow(pushQueue.take());
                } catch (InterruptedException e) {
                    LOGGER.debug("Interrupted trying to get next UpdateTask to push");
                    interrupted = true;
                } catch (Exception t) {
                    LOGGER.info("Caught unexpected exception sending metrics", t);
                }
            }
        }
    }
}
