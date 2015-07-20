package org.backmeup.worker.perfmon;

import com.netflix.servo.Metric;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.MonitorConfig;

/**
 * Poller for standard JVM Runtime metrics.
 */
public class RuntimeMetricPoller extends BaseMetricPoller {
    private static final MonitorConfig TOTAL_MEMORY =
            MonitorConfig.builder("totalMemory")
                    .withTag(DataSourceType.GAUGE)
                    .build();

    private static final MonitorConfig FREE_MEMORY =
            MonitorConfig.builder("freeMemory")
                    .withTag(DataSourceType.GAUGE)
                    .build();

    private static final MonitorConfig USED_MEMORY =
            MonitorConfig.builder("usedMemory")
                    .withTag(DataSourceType.GAUGE)
                    .build();

    private static final MonitorConfig MAX_MEMORY =
            MonitorConfig.builder("maxMemory")
                    .withTag(DataSourceType.GAUGE)
                    .build();

    public RuntimeMetricPoller() {
        
    }

    @Override
    public void addMetricsImpl(long timestamp, MetricList metrics) {
        metrics.add(new Metric(TOTAL_MEMORY, timestamp, Runtime.getRuntime().totalMemory()));
        metrics.add(new Metric(FREE_MEMORY, timestamp, Runtime.getRuntime().freeMemory()));
        metrics.add(new Metric(USED_MEMORY, timestamp, Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        metrics.add(new Metric(MAX_MEMORY, timestamp, Runtime.getRuntime().maxMemory()));
    }
}

