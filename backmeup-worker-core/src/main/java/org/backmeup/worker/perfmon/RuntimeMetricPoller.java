package org.backmeup.worker.perfmon;

import java.util.ArrayList;
import java.util.List;

import com.netflix.servo.Metric;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.publish.MetricFilter;
import com.netflix.servo.publish.MetricPoller;

/**
 * Poller for standard JVM Runtime metrics.
 */
public class RuntimeMetricPoller implements MetricPoller {    
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

    public final List<Metric> poll(MetricFilter filter) {
        return poll(filter, false);
    }

    public final List<Metric> poll(MetricFilter filter, boolean reset) {
        long now = System.currentTimeMillis();
        MetricList metrics = new MetricList(filter);
        addRuntimegMetrics(now, metrics);
        return metrics.getList();
    }

    private void addRuntimegMetrics(long timestamp, MetricList metrics) {
        metrics.add(new Metric(TOTAL_MEMORY, timestamp, Runtime.getRuntime().totalMemory()));
        metrics.add(new Metric(FREE_MEMORY, timestamp, Runtime.getRuntime().freeMemory()));
        metrics.add(new Metric(USED_MEMORY, timestamp, Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        metrics.add(new Metric(MAX_MEMORY, timestamp, Runtime.getRuntime().maxMemory()));
    }

    private static class MetricList {
        private final MetricFilter filter;
        private final List<Metric> list;

        public MetricList(MetricFilter filter) {
            this.filter = filter;
            list = new ArrayList<Metric>();
        }

        public void add(Metric m) {
            if (filter.matches(m.getConfig())) {
                list.add(m);
            }
        }

        public List<Metric> getList() {
            return list;
        }
    }
}

