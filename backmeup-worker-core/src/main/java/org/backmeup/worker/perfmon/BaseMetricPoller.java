package org.backmeup.worker.perfmon;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.Metric;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.publish.MetricFilter;
import com.netflix.servo.publish.MetricPoller;

public abstract class BaseMetricPoller implements MetricPoller {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricPoller.class);

    public BaseMetricPoller() {

    }

    @Override
    public final List<Metric> poll(MetricFilter filter) {
        return poll(filter, false);
    }

    @Override
    public final List<Metric> poll(MetricFilter filter, boolean reset) {
        long now = System.currentTimeMillis();
        MetricList metrics = new MetricList(filter);
        addMetricsImpl(now, metrics);
        return metrics.getList();
    }

    public abstract void addMetricsImpl(long timestamp, MetricList metrics);

    protected final void addMetric(MonitorConfig config, long timestamp, Object obj, String methodName, MetricList metrics) {
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

    static class MetricList {
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
