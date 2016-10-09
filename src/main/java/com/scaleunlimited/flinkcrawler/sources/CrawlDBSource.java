package com.scaleunlimited.flinkcrawler.sources;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("serial")
public class CrawlDBSource implements SourceFunction<Tuple0> {

	private boolean _keepRunning = false;
	
	public CrawlDBSource() {
	}

	@Override
	public void cancel() {
		_keepRunning = false;
	}

	@Override
	public void run(SourceContext<Tuple0> context) throws Exception {
		_keepRunning = true;
		
		while (_keepRunning) {
			context.collect(new Tuple0());
			Thread.sleep(100L);
		}
	}

}
