package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

/**
 * The Flink operator that wraps our "crawl DB". Incoming URLs are de-duplicated and merged in memory.
 * When the in-memory structure is too full, or we need to generate more URLs to fetch, then we merge
 * with an on-disk version; during that process we archive low-scoring URLs and add the highest-scoring
 * URLs to the fetch queue.
 * 
 * We take as input two streams - one is fed by the SeedUrlSource, plus a loop-back Iteration, and this
 * contains actual CrawlStateUrl tuples. The other stream is a timed "trigger" stream that regularly
 * generates an ignorable Tuple that we .
 * 
 */
@SuppressWarnings("serial")
public class CrawlDBFunction extends RichProcessFunction<CrawlStateUrl, FetchUrl> {
    static final Logger LOGGER = LoggerFactory.getLogger(CrawlDBFunction.class);
    
	// TODO configure this
	private static final int MAX_ACTIVE_URLS = 10_000;
	
	// TODO pick good time for this
	private static final long QUEUE_CHECK_DELAY = 10;

	private BaseCrawlDB _crawlDB;
	
	// List of URLs that are available to be fetched.
	private transient FetchQueue _fetchQueue;

	private transient int _parallelism;
	private transient int _index;
	
	public CrawlDBFunction(BaseCrawlDB crawlDB) {
		_crawlDB = crawlDB;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		RuntimeContext context = getRuntimeContext();
		_parallelism = context.getNumberOfParallelSubtasks();
		_index = context.getIndexOfThisSubtask();

		_fetchQueue = new FetchQueue(MAX_ACTIVE_URLS);
		
		_crawlDB.open(_fetchQueue);
	}
	
	@Override
	public void close() throws Exception {
		_crawlDB.close();
		super.close();
	}

	@Override
	public void processElement(CrawlStateUrl url, Context context, Collector<FetchUrl> collector) throws Exception {
		UrlLogger.record(this.getClass(), url, FetchStatus.class.getSimpleName(), url.getStatus().toString());
		
		// Add to our in-memory queue. If this is full, it might trigger a merge.
		// TODO Start the merge in a thread, and use an in-memory array to hold URLs until the merge is done. If this array gets
		// too big, then we need to block until we're done with the merge.
		synchronized (_crawlDB) {
			if (_crawlDB.add(url)) {
				_crawlDB.merge();
			}
		}
		
		// Every time we get called, we'll set up a new timer that fires, which will call the onTimer() method to
		// emit URLs.
		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}
	
	@Override
	public void onTimer(long time, OnTimerContext context, Collector<FetchUrl> collector) throws Exception {
		// TODO do this in a loop?
		FetchUrl url = _fetchQueue.poll();
		
		if (url != null) {
			LOGGER.debug(String.format("CrawlDBFunction emitting URL %s to fetch (partition %d of %d)", url, _index, _parallelism));
			collector.collect(url);
		} else {
			// We don't have any active URLs to fetch.
			// Call the CrawlDB to trigger a merge.
			// TODO if we're merging already
			synchronized (_crawlDB) {
				// We might have done a merge while waiting to get the lock on the _crawlDB, so only
				// do the merge if the fetch queue is still empty.
				if (_fetchQueue.isEmpty()) {
					_crawlDB.merge();
				}
			}
		}

		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}


}
