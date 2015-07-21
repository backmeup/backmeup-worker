package org.backmeup.worker.perfmon;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.backmeup.worker.job.BackupJobRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.Metric;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.publish.MetricObserver;
import com.netflix.servo.tag.Tag;
import com.netflix.servo.tag.TagList;
import com.netflix.servo.util.Preconditions;

/**
 * Converts counter metrics into a rate. The rate is calculated by comparing
 * last two samples of given metric and looking at the delta. Since two samples
 * are needed to calculate the rate, the value of the first sample is stored
 * until a second sample arrives. Then the delta is calculated and the value in
 * the cache is replaced.
 * 
 * <p>Counters should be monotonically increasing values. If a counter value
 * decreases from one sample to the next, then we will assume the counter value
 * was reset and send a rate of 0. This is similar to the RRD concept of type
 * DERIVE with a min of 0.
 */
public final class CounterToRateDeltasTransform implements MetricObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(CounterToRateDeltasTransform.class);

    private static final String COUNTER_VALUE = DataSourceType.COUNTER.name();
    private static final Tag RATE_TAG = DataSourceType.RATE;
    private static final double RATE_ZERO_THRESHOLD = 0.000001;

    private final MetricObserver observer;
    private final Map<MonitorConfig, CounterValue> cache;


    /**
     * Creates a new instance with the specified downstream observer.
     *
     * @param observer
     *            downstream observer to forward values to after the rate has
     *            been computed.
     */
    public CounterToRateDeltasTransform(MetricObserver observer) {
        this.observer = observer;
        this.cache = new LinkedHashMap<MonitorConfig, CounterValue>(16, 0.75f, true);
    }

    public String getName() {
        return observer.getName();
    }

    /** {@inheritDoc} */
    public void update(List<Metric> metrics) {
        Preconditions.checkNotNull(metrics, "metrics");
        LOGGER.debug("received {} metrics", metrics.size());
        final List<Metric> newMetrics = new ArrayList<Metric>(metrics.size());
        for (Metric m : metrics) {
            if (isCounter(m) && isJobMetric(m)) {
                final MonitorConfig rateConfig = toRateConfig(m.getConfig());
                final CounterValue prev = cache.get(rateConfig);
                if (prev != null) {
                    final double rate = prev.computeRate(m);
                    if (!isZero(rate, RATE_ZERO_THRESHOLD)) {
                        newMetrics.add(new Metric(rateConfig, m.getTimestamp(), rate));
                    }
                } else {
                    CounterValue current = new CounterValue(m);
                    cache.put(rateConfig, current);
                }
            } else {
                newMetrics.add(m);
            }
        }
        LOGGER.debug("writing {} metrics to downstream observer", newMetrics.size());
        observer.update(newMetrics);
    }

    /**
     * Clear all cached state of previous counter values.
     */
    public void reset() {
        cache.clear();
    }

    /**
     * Convert a MonitorConfig for a counter to one that is explicit about being
     * a RATE.
     */
    private MonitorConfig toRateConfig(MonitorConfig config) {
        return config.withAdditionalTag(RATE_TAG);
    }

    private boolean isCounter(Metric m) {
        final TagList tags = m.getConfig().getTags();
        final String value = tags.getValue(DataSourceType.KEY);
        return value != null && COUNTER_VALUE.equals(value);
    }
    
    private boolean isJobMetric(Metric m) {
        final TagList tags = m.getConfig().getTags();
        final String classTag = tags.getValue(JobMetrics.CLASS_TAG);
        return classTag != null && classTag.equals(BackupJobRunner.class.getSimpleName());
    }
    
    private boolean isZero(double value, double threshold){
        return value >= -threshold && value <= threshold;
    }

    private static class CounterValue {
        private double value;

        public CounterValue(double value) {
            this.value = value;
        }

        public CounterValue(Metric m) {
            this(m.getNumberValue().doubleValue());
        }

        public double computeRate(Metric m) {
            final double currentValue = m.getNumberValue().doubleValue();
            final double delta = currentValue - value;
            value = currentValue;
            return (delta <= 0.0) ? 0.0 : delta;
        }
    }
}
