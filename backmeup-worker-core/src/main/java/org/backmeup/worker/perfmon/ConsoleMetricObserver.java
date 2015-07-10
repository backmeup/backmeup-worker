package org.backmeup.worker.perfmon;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.Metric;
import com.netflix.servo.publish.BaseMetricObserver;

public class ConsoleMetricObserver extends BaseMetricObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleMetricObserver.class);
    
    private final String metricPrefix;

    public ConsoleMetricObserver(String metricPrefix) {
        super("BackmeupMetricObserver");
        this.metricPrefix = metricPrefix;
    }

    @Override
    public void updateImpl(List<Metric> metrics) {
        try {
            write(metrics);
        } catch (Exception e) {
            LOGGER.warn("Graphite connection failed on write", e);
            incrementFailedCount();
        }
    }

    private void write(List<Metric> metrics) throws IOException {
        PrintWriter writer = new PrintWriter(System.out);
        int count = writeMetrics(metrics, writer);
        if (writer.checkError()) {
            throw new IOException("Writing metrics has failed");
        }

        LOGGER.debug("Wrote {} metrics to Backmeup-Service", count);
    }

    private int writeMetrics(List<Metric> metrics, PrintWriter writer) {
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
            LOGGER.debug("{}", sb);
            writer.write(sb.append("\n").toString());
            count++;
        }
        return count;
    }
}
