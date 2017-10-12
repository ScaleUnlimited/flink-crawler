package com.scaleunlimited.flinkcrawler.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.scaleunlimited.flinkcrawler.pojos.TicklerTuple;

@SuppressWarnings("serial")
public class TicklerSource extends RichParallelSourceFunction<TicklerTuple> {

	public static final long NO_MAX_DURATION = -1;
	
	private static final long TICKLE_INTERVAL = 100L;
	
	private volatile boolean _keepRunning = true;
	private long _maxDuration;
	
	private transient long _endTime;
	
	public TicklerSource(long maxDuration) {
		_maxDuration = maxDuration;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		if (_maxDuration == NO_MAX_DURATION) {
			_endTime = Long.MAX_VALUE;
		} else {
			_endTime = System.currentTimeMillis() + _maxDuration;
		}
	}
	
	@Override
	public void cancel() {
		_keepRunning = false;
	}

	@Override
	public void run(SourceContext<TicklerTuple> context) throws Exception {
		
		while (_keepRunning && (System.currentTimeMillis() < _endTime)) {
			context.collect(new TicklerTuple());
			Thread.sleep(TICKLE_INTERVAL);
		}
	}

}
