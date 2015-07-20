package org.backmeup.worker.perfmon;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.Metric;
import com.netflix.servo.publish.BaseMetricObserver;

public class LoggerMetricObserver extends BaseMetricObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerMetricObserver.class);
    
    private final String metricPrefix;

    public LoggerMetricObserver(String metricPrefix) {
        super("BackmeupMetricObserver");
        this.metricPrefix = metricPrefix;
    }

    @Override
    public void updateImpl(List<Metric> metrics) {
        int count = writeMetrics(metrics);
        LOGGER.debug("Wrote {} metrics to Backmeup-Service", count);
    }

    private int writeMetrics(List<Metric> metrics) {
        int count = 0;
        for (Metric metric : metrics) {
            String publishedName = metric.getConfig().getName();

            StringBuilder sb = new StringBuilder();
            if (metricPrefix != null) {
                sb.append(metricPrefix).append(".");
            }
            sb.append(publishedName)
              .append(" ")
              .append(metric.getValue().toString())
              .append(" ")
              .append(metric.getTimestamp() / 1000);
            LOGGER.info("{}", sb);
            count++;
        }
        return count;
    }
}
