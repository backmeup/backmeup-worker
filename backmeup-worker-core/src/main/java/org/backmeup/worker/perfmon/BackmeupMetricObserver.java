package org.backmeup.worker.perfmon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.backmeup.model.dto.WorkerMetricDTO;
import org.backmeup.service.client.BackmeupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.Metric;
import com.netflix.servo.publish.BaseMetricObserver;

public class BackmeupMetricObserver extends BaseMetricObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackmeupMetricObserver.class);
    private static final String METRIC_PREFIX_DEFAULT = "backmeup.worker";
    private static final String OBSERVER_NAME_DEFAULT = "BackmeupMetricObserver";

    private final BackmeupService backmeupServiceClient;

    private volatile boolean sendMetrics = true;
    
    @SuppressWarnings("unused")
    private final String metricPrefix;

    public BackmeupMetricObserver(BackmeupService backmeupServiceClient) {
        this(backmeupServiceClient, METRIC_PREFIX_DEFAULT);
    }

    public BackmeupMetricObserver(BackmeupService backmeupServiceClient, String metricPrefix) {
        super(OBSERVER_NAME_DEFAULT);
        this.backmeupServiceClient = backmeupServiceClient;
        this.metricPrefix = metricPrefix;
    }
    
    public void stop() {
        if (sendMetrics) {
            sendMetrics = false;
            LOGGER.info("Stop sending metrics to backmeup-service");
        }
    }

    @Override
    public void updateImpl(List<Metric> metrics) {
        if (!sendMetrics) {
            LOGGER.debug("BackmeupMetricObserver is disabled. Not sending metrics.");
            return;
        }

        if (metrics.isEmpty()) {
            LOGGER.debug("List of metrics is empty. No metrics to send.");
            return;
        }
        
        try {
            LOGGER.debug("Sending {} metrics to backmeup-service ({})", metrics.size(), "http://localhost...");
            sendMetrics(metrics);
        } catch (IOException e) {
            LOGGER.warn("Graphite connection failed on write", e);
            incrementFailedCount();
        }
    }

    private void sendMetrics(List<Metric> metrics) throws IOException {
        List<WorkerMetricDTO> workerMetrics = new ArrayList<>(metrics.size());
        for(Metric m : metrics) {
            WorkerMetricDTO metric = new WorkerMetricDTO();
            metric.setTimestamp(new Date(m.getTimestamp()));
            metric.setMetric( m.getConfig().getName());
            metric.setValue(m.getNumberValue().doubleValue());
            workerMetrics.add(metric);
        }
        
        this.backmeupServiceClient.addWorkerMetrics(workerMetrics);
    }
}
