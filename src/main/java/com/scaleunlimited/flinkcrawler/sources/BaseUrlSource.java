package com.scaleunlimited.flinkcrawler.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public abstract class BaseUrlSource extends RichSourceFunction<RawUrl> {

	private transient int _maxParallelism;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		StreamingRuntimeContext context = (StreamingRuntimeContext)getRuntimeContext();
		_maxParallelism = context.getMaxNumberOfParallelSubtasks();
	}
	
	protected int getMaxParallelism() {
	    return _maxParallelism;
	}
}
