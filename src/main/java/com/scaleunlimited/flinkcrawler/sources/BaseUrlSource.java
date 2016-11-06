package com.scaleunlimited.flinkcrawler.sources;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public abstract class BaseUrlSource extends RichParallelSourceFunction<RawUrl> {

	protected transient int _index;
	protected transient int _parallelism;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		RuntimeContext context = getRuntimeContext();
		_parallelism = context.getNumberOfParallelSubtasks();
		_index = context.getIndexOfThisSubtask();
	}
}
