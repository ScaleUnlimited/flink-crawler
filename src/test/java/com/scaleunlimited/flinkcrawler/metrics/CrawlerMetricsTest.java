package com.scaleunlimited.flinkcrawler.metrics;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.scaleunlimited.flinkcrawler.functions.BaseMapFunction;

public class CrawlerMetricsTest {
    
    public static class TestReporter implements MetricReporter {
        
        private static Map<String, Metric> _metrics;

        public TestReporter() {
            super();
        }
        
        public static Metric getMetric(Enum<?> e) {
            return _metrics.get(getMetricIdentifier(e));
        }
        
        private static Object getMetricIdentifier(Enum<?> e) {
            String groupName = CounterUtils.enumToGroup(e);
            String metricName = CounterUtils.enumToCounter(e);
            return (groupName + "." + metricName);
        }

        @Override
        public void open(MetricConfig metricConfig) {
            _metrics = new HashMap<String, Metric>();
        }

        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            _metrics.put(group.getMetricIdentifier(metricName), metric);
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
            _metrics.remove(group.getMetricIdentifier(metricName));
        }
        
        @Override
        public void close() {
        }
    }

    @SuppressWarnings("serial")
    private static class IdentityFunction extends BaseMapFunction<String, String> {

        @Override
        public String map(String inputString) throws Exception {
            CounterUtils.increment(getRuntimeContext(), CrawlerMetrics.COUNTER_PAGES_PARSED);
            return inputString;
        }
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test() throws Throwable {
        Configuration config = new Configuration();
        config.setString("metrics.reporters", "test_reporter");
        config.setString("metrics.reporter.test_reporter.class", TestReporter.class.getName());
        LocalStreamEnvironment env = new LocalStreamEnvironment(config);
        DataStreamSource<String> input = env.fromElements("one", "two", "three");
        input.map(new IdentityFunction()).print();

        try {
            env.execute();
        } catch (JobExecutionException e) {
            Assert.fail(e.getCause().getMessage());
        }
        
//        Counter pagesParsedCounter = (Counter)(TestReporter.getMetric(CrawlerMetrics.COUNTER_PAGES_PARSED));
//        Assert.assertEquals(3L, pagesParsedCounter.getCount());
    }
}
