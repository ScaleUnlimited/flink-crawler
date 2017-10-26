package com.scaleunlimited.flinkcrawler.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;

/**
 * Generate special CrawlStateUrl values (ticklers) that keep the iteration
 * running, until we're stopped. When that happens, generate termination
 * CrawStateUrls that tell the CrawlDB to flush the fetch queue and not
 * fill it any longer.
 * 
 * Note that the parallelism of this source MUST match the parallelism of
 * the CrawlDBFunction, so that it generates values which will be partitioned
 * to all of the CrawlDBFunction tasks.
 *
 */
@SuppressWarnings("serial")
public class TicklerSource extends RichParallelSourceFunction<CrawlStateUrl> {
	private static final Logger LOGGER = LoggerFactory.getLogger(TicklerSource.class);

	public static final long TICKLE_INTERVAL = 100L;
	
	private volatile boolean _keepRunning = true;
	
	private transient int _index;
	
	public TicklerSource() {
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_index = getRuntimeContext().getIndexOfThisSubtask();
		LOGGER.debug("Opening TicklerSource for partition " + _index);
	}
	
	@Override
	public void cancel() {
		_keepRunning = false;
	}

	@Override
	public void run(SourceContext<CrawlStateUrl> context) throws Exception {
		
		while (_keepRunning) {
			LOGGER.trace("Emitting tickler URL for partition " + _index);

			context.collect(CrawlStateUrl.makeTicklerUrl(_index));
			Thread.sleep(TICKLE_INTERVAL);
		}
		
		LOGGER.debug("Stopping TicklerSource for partition " + _index);
		
		// TODO once we terminate, start emitting termination URLs until our
		// termination time interval is passed.
	}

}
