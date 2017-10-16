package com.scaleunlimited.flinkcrawler.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

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

	// Quiet time that implies no check for lack of activity.
	public static final long NO_QUIET_TIME = -1;
	
	private volatile boolean _keepRunning = true;
	private long _maxQuietTime;
	
	private transient int _index;
	
	public TicklerSource(long maxQuietTime) {
		_maxQuietTime = maxQuietTime;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_index = getRuntimeContext().getIndexOfThisSubtask();
	}
	
	@Override
	public void cancel() {
		_keepRunning = false;
	}

	@Override
	public void run(SourceContext<CrawlStateUrl> context) throws Exception {
		
		while (_keepRunning) {
			context.collect(CrawlStateUrl.makeTicklerUrl(_index));
			Thread.sleep(TICKLE_INTERVAL);
			
			// Now check quiet time, but only if it's set. Note that this only
			// is going to work in a single JVM (test) environment.
			long lastActivityTime = UrlLogger.getLastActivityTime();
			if ((_maxQuietTime != NO_QUIET_TIME) && (lastActivityTime != UrlLogger.NO_ACTIVITY_TIME)) {
				long curTime = System.currentTimeMillis();
				if ((curTime - lastActivityTime) > _maxQuietTime) {
					LOGGER.info("It's been too quiet, terminating");
					_keepRunning = false;
				}
			}
		}
		
		// TODO once we terminate, start emitting termination URLs until our
		// termination time interval is passed.
	}

}
