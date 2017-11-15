package com.scaleunlimited.flinkcrawler.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;

public class CrawlerAccumulator {

	private RuntimeContext _runtimeContext;

	public CrawlerAccumulator(RuntimeContext runtimeContext) {
		_runtimeContext = runtimeContext;
	}
	
	public void increment(Enum<?> e) {
		CounterUtils.increment(_runtimeContext, e);
	}
	

	public void increment(Enum<?> e, long l) {
		CounterUtils.increment(_runtimeContext, e, l);
	}
	
	public void increment(String group, String counter, long l) {
		CounterUtils.increment(_runtimeContext, group, counter, l);
	}

}
