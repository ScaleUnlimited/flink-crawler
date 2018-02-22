package com.scaleunlimited.flinkcrawler.utils;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

public class FlinkUtilsTest {

	@Test
	public void testMakeKeyForOperatorIndex() throws Exception {
		final int parallelism = 2;
		LocalStreamEnvironment env = new LocalStreamEnvironment();
		env.setParallelism(parallelism);
		
		final int maxParallelism = env.getMaxParallelism();

		DataStreamSource<Tuple2<String, Float>> pages = env.fromElements(Tuple2.of("page0", 0.0f), Tuple2.of("page0", 1.0f), Tuple2.of("page1", 10.0f), Tuple2.of("page666", 6660.0f));
		DataStreamSource<Tuple2<String, Float>> epsilon = env.fromElements(
				Tuple2.of("task:" + FlinkUtils.makeKeyForOperatorIndex(maxParallelism, parallelism, 0), 0.5f), 
				Tuple2.of("task:" + FlinkUtils.makeKeyForOperatorIndex(maxParallelism, parallelism, 1), 0.25f));
		
		pages.union(epsilon).keyBy(new MyKeySelector()).process(new MyProcessFunction()).print();
		
		try {
			env.execute();
		} catch (JobExecutionException e) {
			Assert.fail(e.getCause().getMessage());
		}
	}
	
	@SuppressWarnings("serial")
	private static class MyKeySelector implements KeySelector<Tuple2<String, Float>, Integer> {

		@Override
		public Integer getKey(Tuple2<String, Float> tuple) throws Exception {
			if (tuple.f0.startsWith("task:")) {
				return Integer.parseInt(tuple.f0.substring("task:".length()));
			} else {
				return tuple.f0.hashCode();
			}
		}
	}
	
	@SuppressWarnings("serial")
	private static class MyProcessFunction extends ProcessFunction<Tuple2<String, Float>, Tuple2<String, Float>> {

		private transient int _numTaskRecords;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			
			_numTaskRecords = 0;
		}
		
		@Override
		public void close() throws Exception {
			super.close();
			
			if (_numTaskRecords != 1) {
				throw new RuntimeException("Expected 1 task record, got " + _numTaskRecords);
			}
		}
		
		@Override
		public void processElement(Tuple2<String, Float> in, Context context, Collector<Tuple2<String, Float>> collector) throws Exception {
			collector.collect(in);
			
			if (in.f0.startsWith("task:")) {
				_numTaskRecords += 1;
			}
		}
	}

}
