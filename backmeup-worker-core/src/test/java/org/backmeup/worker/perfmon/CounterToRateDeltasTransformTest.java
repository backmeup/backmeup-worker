package org.backmeup.worker.perfmon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.backmeup.worker.job.BackupJobRunner;
import org.junit.Test;

import com.netflix.servo.Metric;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.publish.MemoryMetricObserver;
import com.netflix.servo.publish.MetricObserver;
import com.netflix.servo.tag.SortedTagList;
import com.netflix.servo.tag.TagList; 


public class CounterToRateDeltasTransformTest {
    private static final double DELTA_THRESHOLD = 0.00001;

    private static final TagList GAUGE = SortedTagList.builder().withTag(DataSourceType.GAUGE).build(); 
    
    private static final TagList COUNTER = SortedTagList.builder().withTag(DataSourceType.COUNTER).withTag(JobMetrics.CLASS_TAG, BackupJobRunner.class.getSimpleName()).build(); 
 
    private List<Metric> mkList(long ts, int value) {
        List<Metric> metrics = new ArrayList<Metric>();
        metrics.add(new Metric("m1", SortedTagList.EMPTY, ts, value));
        metrics.add(new Metric("m2", GAUGE, ts, value));
        metrics.add(new Metric("m3", COUNTER, ts, value));
        return Collections.unmodifiableList(metrics);
    }
 
    private Map<String,Double> mkMap(List<List<Metric>> updates) { 
        Map<String,Double> map = new HashMap<>();
        for (Metric m : updates.get(0)) { 
            map.put(m.getConfig().getName(), m.getNumberValue().doubleValue()); 
        } 
        return map; 
    } 
 
    @Test 
    public void testSimpleRate() throws Exception { 
        MemoryMetricObserver mmo = new MemoryMetricObserver("m", 1); 
        MetricObserver transform = new CounterToRateDeltasTransform(mmo); 
        Map<String,Double> metrics; 
 
        // Make time look like the future to avoid expirations 
        long baseTime = System.currentTimeMillis() + 100000L; 
 
        // First sample 
        transform.update(mkList(baseTime + 0, 0)); 
        metrics = mkMap(mmo.getObservations()); 
        assertEquals(2, metrics.size()); 
        assertTrue(metrics.get("m3") == null); 
 
        // Delta of 5 
        transform.update(mkList(baseTime + 5000, 5)); 
        metrics = mkMap(mmo.getObservations()); 
        assertEquals(3, metrics.size()); 
        assertEquals(5.0, metrics.get("m3"), DELTA_THRESHOLD); 
 
        // Delta of 15 
        transform.update(mkList(baseTime + 10000, 20)); 
        metrics = mkMap(mmo.getObservations()); 
        assertEquals(3, metrics.size()); 
        assertEquals(15, metrics.get("m3"), DELTA_THRESHOLD); 
 
        // No change from previous sample 
        transform.update(mkList(baseTime + 15000, 20)); 
        metrics = mkMap(mmo.getObservations()); 
        assertEquals(2, metrics.size()); 
        assertTrue(metrics.get("m3") == null); 
 
        // Decrease from previous sample
        transform.update(mkList(baseTime + 20000, 19)); 
        metrics = mkMap(mmo.getObservations()); 
        assertEquals(2, metrics.size()); 
        assertTrue(metrics.get("m3") == null); 
        
        // Delta of 31, value of last sample was 19
        transform.update(mkList(baseTime + 25000, 50)); 
        metrics = mkMap(mmo.getObservations()); 
        assertEquals(3, metrics.size()); 
        assertEquals(31, metrics.get("m3"), DELTA_THRESHOLD); 
    } 
}
