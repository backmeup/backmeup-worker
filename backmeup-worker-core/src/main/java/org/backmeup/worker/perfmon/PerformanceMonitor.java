package org.backmeup.worker.perfmon;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.backmeup.service.client.BackmeupService;

import com.netflix.servo.publish.AsyncMetricObserver;
import com.netflix.servo.publish.BasicMetricFilter;
import com.netflix.servo.publish.FileMetricObserver;
import com.netflix.servo.publish.MemoryMetricObserver;
import com.netflix.servo.publish.MetricObserver;
import com.netflix.servo.publish.MetricPoller;
import com.netflix.servo.publish.MonitorRegistryMetricPoller;
import com.netflix.servo.publish.PollRunnable;
import com.netflix.servo.publish.PollScheduler;

public final class PerformanceMonitor {
    private static final long HEARTBEAT_INTERVAL = 10;
    private static List<MetricObserver> observers = new ArrayList<MetricObserver>();
    
    private PerformanceMonitor() {
        
    }

    public static void initialize() {
        observers.add(createMemoryMetricObserver());
    }
    
    public static void initialize(String fileName) {
        observers.add(createFileObserver(fileName));
    }

    public static void initialize(BackmeupService bmuServiceClient) {
        observers.add(createBackmeupMetricObserver(bmuServiceClient));
    }

    public static void startPublishing() {
        if (observers.isEmpty()) {
            throw new IllegalStateException("No observer initialized");
        }

        PollScheduler.getInstance().start();
        schedule(new MonitorRegistryMetricPoller(), observers);
        schedule(new OperatingSystemMetricPoller(), observers);
        schedule(new RuntimeMetricPoller(), observers);
    }
    
    private static MetricObserver createMemoryMetricObserver() {
        return transformCountertoRate(new MemoryMetricObserver());
    }

    private static MetricObserver createFileObserver(String fileName) {
        String prefix = fileName;
        if (prefix.isEmpty()) {
            prefix = "backmeup-worker_metrics_";
        }
        return async("AsyncFileMetricObserver", transformCountertoRate(
                new FileMetricObserver(prefix, new File("."))));
    }

    private static MetricObserver createBackmeupMetricObserver(
            BackmeupService bmuServiceClient) {
        return async("AsyncBackmeupMetricObserver", transformCountertoRate(
                new BackmeupMetricObserver(bmuServiceClient)));
    }

    private static MetricObserver transformCountertoRate(
            MetricObserver observer) {
        return new CounterToRateDeltasTransform(observer);
    }

    private static MetricObserver async(String name, MetricObserver observer) {
        final long expireTime = 2000 * HEARTBEAT_INTERVAL;
        final int queueSize = 10;
        return new AsyncMetricObserver(name, observer, queueSize, expireTime);
    }

    private static void schedule(MetricPoller poller,
            List<MetricObserver> observers) {
        final PollRunnable task = new PollRunnable(poller,
                BasicMetricFilter.MATCH_ALL, true, observers);
        PollScheduler.getInstance().addPoller(task, HEARTBEAT_INTERVAL,
                TimeUnit.SECONDS);
    }
}
