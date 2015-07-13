package org.backmeup.worker.perfmon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.netflix.servo.Metric;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.publish.BasicMetricFilter;
import com.netflix.servo.publish.MetricPoller;

public class OperatingSystemMetricPollerTest {
    @Test
    public void testBasic() throws Exception {
        MetricPoller poller = new OperatingSystemMetricPoller();

        boolean found = false;
        List<Metric> metrics = poller.poll(BasicMetricFilter.MATCH_ALL);
        for (Metric m : metrics) {
            if ("totalPhysicalMemory".equals(m.getConfig().getName())) {
                Map<String, String> tags = m.getConfig().getTags().asMap();;
                assertEquals(tags.get(DataSourceType.KEY), "GAUGE");
                found = true;
            }
        }
        assertTrue(found);
    }
}
