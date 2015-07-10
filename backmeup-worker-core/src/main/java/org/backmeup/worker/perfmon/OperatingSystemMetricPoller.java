package org.backmeup.worker.perfmon;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.Metric;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.publish.MetricFilter;
import com.netflix.servo.publish.MetricPoller;

public class OperatingSystemMetricPoller implements MetricPoller {

    private static final MonitorConfig TOTAL_PHYSICAL_MEMORY_SIZE = MonitorConfig
            .builder("totalPhysicalMemory").withTag(DataSourceType.GAUGE)
            .build();

    private static final MonitorConfig FREE_PHYSICAL_MEMORY_SIZE = MonitorConfig
            .builder("freePhysicalMemory").withTag(DataSourceType.GAUGE)
            .build();

    private static final MonitorConfig PROCESS_CPU_LOAD = MonitorConfig
            .builder("processCpuLoad").withTag(DataSourceType.GAUGE).build();

    private static final MonitorConfig SYSTEM_CPU_LOAD = MonitorConfig
            .builder("systemCpuLoad").withTag(DataSourceType.GAUGE).build();

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatingSystemMetricPoller.class);

    public OperatingSystemMetricPoller() {

    }

    @Override
    public final List<Metric> poll(MetricFilter filter) {
        return poll(filter, false);
    }

    @Override
    public final List<Metric> poll(MetricFilter filter, boolean reset) {
        long now = System.currentTimeMillis();
        MetricList metrics = new MetricList(filter);
        addOperatingSystemMetrics(now, metrics);
        return metrics.getList();
    }

    private void addOperatingSystemMetrics(long timestamp, MetricList metrics) {
        OperatingSystemMXBean bean = ManagementFactory
                .getOperatingSystemMXBean();

        addOptionalMetric(TOTAL_PHYSICAL_MEMORY_SIZE, timestamp, bean,
                "getTotalPhysicalMemorySize", metrics);
        addOptionalMetric(FREE_PHYSICAL_MEMORY_SIZE, timestamp, bean,
                "getFreePhysicalMemorySize", metrics);
        addOptionalMetric(PROCESS_CPU_LOAD, timestamp, bean,
                "getProcessCpuLoad", metrics);
        addOptionalMetric(SYSTEM_CPU_LOAD, timestamp, bean, "getSystemCpuLoad",
                metrics);
    }

    private void addOptionalMetric(MonitorConfig config, long timestamp,
            Object obj, String methodName, MetricList metrics) {
        try {
            Method method = obj.getClass().getMethod(methodName);
            method.setAccessible(true);
            Number value = (Number) method.invoke(obj);
            metrics.add(new Metric(config, timestamp, value));
        } catch (Exception e) {
            final String msg = String.format("failed to get value for %s.%s",
                    obj.getClass().getName(), methodName);
            LOGGER.debug(msg, e);
        }
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
