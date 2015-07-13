package org.backmeup.worker.perfmon;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.MonitorConfig;

public final class JobMetrics {
    public static final String CLASS_TAG = "class";
    
    public static final String BYTES_RECEIVED = "bytesReceived";
    public static final String BYTES_SENT = "bytesSent";

    public static final String OBJECTS_RECEIVED = "objectsReceived";
    public static final String OBJECTS_SENT = "objectsSent";

    private static final ConcurrentMap<MonitorConfig, Counter> counters = new ConcurrentHashMap<MonitorConfig, Counter>();

    public static Counter getCounter(MonitorConfig config) {
        Counter v = counters.get(config);
        if (v != null)
            return v;
        else {
            Counter counter = new BasicCounter(config);
            Counter prevCounter = counters.putIfAbsent(config, counter);
            if (prevCounter != null)
                return prevCounter;
            else {
                DefaultMonitorRegistry.getInstance().register(counter);
                return counter;
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static Counter getCounter(Class c, String n) {
        MonitorConfig config = MonitorConfig.builder(n).withTag(CLASS_TAG, c.getSimpleName()).build();
        return getCounter(config);
    }
}
