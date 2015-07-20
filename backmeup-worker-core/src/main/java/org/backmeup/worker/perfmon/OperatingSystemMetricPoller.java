package org.backmeup.worker.perfmon;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.MonitorConfig;

public class OperatingSystemMetricPoller extends BaseMetricPoller {

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

    public OperatingSystemMetricPoller() {

    }

    @Override
    public void addMetricsImpl(long timestamp, MetricList metrics) {
        OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();

        addMetric(TOTAL_PHYSICAL_MEMORY_SIZE, timestamp, bean, "getTotalPhysicalMemorySize", metrics);
        addMetric(FREE_PHYSICAL_MEMORY_SIZE, timestamp, bean, "getFreePhysicalMemorySize", metrics);
        addMetric(PROCESS_CPU_LOAD, timestamp, bean, "getProcessCpuLoad", metrics);
        addMetric(SYSTEM_CPU_LOAD, timestamp, bean, "getSystemCpuLoad", metrics);
    }
}
