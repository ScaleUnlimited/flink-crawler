package com.scaleunlimited.flinkcrawler.functions;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB;
import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.TicklerTuple;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

/**
 * The Flink operator that wraps our "crawl DB". Incoming URLs are de-duplicated and merged in memory.
 * When the in-memory structure is too full, or we need to generate more URLs to fetch, then we merge
 * with an on-disk version; during that process we archive low-scoring URLs and add the highest-scoring
 * URLs to the fetch queue.
 */
@SuppressWarnings("serial")
public class CrawlDBFunction extends RichCoFlatMapFunction<CrawlStateUrl, TicklerTuple, FetchUrl> {
    static final Logger LOGGER = LoggerFactory.getLogger(CrawlDBFunction.class);

	private BaseCrawlDB _crawlDB;
	private BaseCrawlDBMerger _merger;
	
	// List of URLs that are available to be fetched.
	private final FetchQueue _fetchQueue;
	
	private transient int _parallelism;
	private transient int _partition;
	
	// True if merging would do anything.
	private transient AtomicBoolean _addSinceMerge;
	
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
		_partition = context.getIndexOfThisSubtask();
		
		_addSinceMerge = new AtomicBoolean(false);
		
		_fetchQueue.open();
		
		_crawlDB.open(_partition, _fetchQueue, _merger);
	}
	
	@Override
	public void close() throws Exception {
		_crawlDB.close();
		super.close();
	}

	@Override
	public void flatMap1(CrawlStateUrl url, Collector<FetchUrl> collector) throws Exception {
		UrlLogger.record(this.getClass(), url, FetchStatus.class.getSimpleName(), url.getStatus().toString());
		
		LOGGER.debug(String.format("CrawlDBFunction processing URL %s (partition %d of %d)", url, _partition, _parallelism));
		
		// Add to our in-memory queue. If this is full, it might trigger a merge.
		// TODO Start the merge in a thread, and use an in-memory array to hold URLs until the merge is done. If this array gets
		// too big, then we need to block until we're done with the merge.
		synchronized (_crawlDB) {
			
			// TODO trigger a merge when the _fetchQueue hits a low water mark, but only if we have something to merge, of course.
			if (_crawlDB.add(url)) {
				LOGGER.debug("CrawlDBFunction merging crawlDB");
				_crawlDB.merge();
				_addSinceMerge.set(false);
			} else {
				_addSinceMerge.set(true);
			}
		}
	}

	@Override
	public void flatMap2(TicklerTuple tickler, Collector<FetchUrl> collector) throws Exception {
		FetchUrl url = _fetchQueue.poll();

		if (url != null) {
			LOGGER.debug(String.format("CrawlDBFunction emitting URL %s to fetch (partition %d of %d)", url, _partition, _parallelism));
			collector.collect(url);
		}

		synchronized (_crawlDB) {
			if (_fetchQueue.isEmpty() && _addSinceMerge.get()) {
				// We don't have any active URLs left.
				// Call the CrawlDB to trigger a merge.
				LOGGER.debug("CrawlDBFunction merging crawlDB");
				_crawlDB.merge();
				_addSinceMerge.set(false);
			}
		}
	}

}
