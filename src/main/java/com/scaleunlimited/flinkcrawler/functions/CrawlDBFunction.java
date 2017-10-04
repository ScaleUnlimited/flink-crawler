package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB;
import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.utils.CounterUtils;
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
public class CrawlDBFunction extends ProcessFunction<CrawlStateUrl, FetchUrl> {
    static final Logger LOGGER = LoggerFactory.getLogger(CrawlDBFunction.class);
    
	// TODO pick good time for this
	private static final long QUEUE_CHECK_DELAY = 100L;

	private static final int URLS_PER_TIMER = 1000;

	private BaseCrawlDB _crawlDB;
	private BaseCrawlDBMerger _merger;
	
	// List of URLs that are available to be fetched.
	private final FetchQueue _fetchQueue;
	
	private transient int _parallelism;
	private transient int _index;
	
	public CrawlDBFunction(BaseCrawlDB crawlDB, BaseCrawlDBMerger merger, FetchQueue fetchQueue) {
		_crawlDB = crawlDB;
		_merger = merger;
		_fetchQueue = fetchQueue;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		RuntimeContext context = getRuntimeContext();
		_parallelism = context.getNumberOfParallelSubtasks();
		_index = context.getIndexOfThisSubtask();
		
		_fetchQueue.open();
		
		_crawlDB.open(_index, _fetchQueue, _merger);
		
	}
	
	@Override
	public void close() throws Exception {
		_crawlDB.close();
		super.close();
	}

	@Override
	public void processElement(CrawlStateUrl url, Context context, Collector<FetchUrl> collector) throws Exception {
		UrlLogger.record(this.getClass(), url, FetchStatus.class.getSimpleName(), url.getStatus().toString());
		
		
		CounterUtils.increment(getRuntimeContext(), url.getStatus());
		// Add to our in-memory queue. If this is full, it might trigger a merge.
		// TODO Start the merge in a thread, and use an in-memory array to hold URLs until the merge is done. If this array gets
		// too big, then we need to block until we're done with the merge.
		synchronized (_crawlDB) {
			// TODO trigger a merge when the _fetchQueue hits a low water mark, but only if we have something to merge, of course.
			if (_crawlDB.add(url)) {
				_crawlDB.merge();
			}
		}
		
		// Every time we get called, we'll set up a new timer that fires, which will call the onTimer() method to emit URLs.
		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}
	
	@Override
	public void onTimer(long time, OnTimerContext context, Collector<FetchUrl> collector) throws Exception {
		for (int i = 0; i < URLS_PER_TIMER; i++) {
			FetchUrl url = _fetchQueue.poll();

			if (url != null) {
				LOGGER.debug(String.format("CrawlDBFunction emitting URL %s to fetch (partition %d of %d)", url, _index, _parallelism));
				collector.collect(url);
			} else {
				break;
			}
		}
		
		synchronized (_crawlDB) {
			if (_fetchQueue.isEmpty()) {
				// We don't have any active URLs left.
				// Call the CrawlDB to trigger a merge.
				LOGGER.debug("CrawlDBFunction merging crawlDB");
				_crawlDB.merge();
			}
		}

		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}


}
