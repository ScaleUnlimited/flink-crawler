package com.scaleunlimited.flinkcrawler.functions;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB;
import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.UrlType;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;

/**
 * The Flink operator that wraps our "crawl DB". Incoming URLs are de-duplicated and merged in memory.
 * When the in-memory structure is too full, or we need to generate more URLs to fetch, then we merge
 * with an on-disk version; during that process we archive low-scoring URLs and add the highest-scoring
 * URLs to the fetch queue.
 */
@SuppressWarnings("serial")
public class CrawlDBFunction extends BaseFlatMapFunction<CrawlStateUrl, FetchUrl> {
    static final Logger LOGGER = LoggerFactory.getLogger(CrawlDBFunction.class);

    private static final String GAUGE_URLS_IN_FETCH_QUEUE = "URLsInFetchQueue";
    private static final int URLS_PER_TICKLE = 100;
	private BaseCrawlDB _crawlDB;
	private BaseCrawlDBMerger _merger;
	
	// List of URLs that are available to be fetched.
	private final FetchQueue _fetchQueue;
	
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
		
		context.getMetricGroup().gauge(GAUGE_URLS_IN_FETCH_QUEUE, new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return _fetchQueue.size();
			}
		});
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
	public void flatMap(CrawlStateUrl url, Collector<FetchUrl> collector) throws Exception {
		
		boolean needMerge = false;
		if (url.getUrlType() == UrlType.REGULAR) {
			record(this.getClass(), url, FetchStatus.class.getSimpleName(), url.getStatus().toString());
			needMerge = _crawlDB.add(url);
			_addSinceMerge.set(true);
		} else if (url.getUrlType() == UrlType.TICKLER) {
			for (int i = 0; i < URLS_PER_TICKLE; i++) {
				FetchUrl fetchUrl = _fetchQueue.poll();

				if (fetchUrl != null) {
					LOGGER.debug(String.format("CrawlDBFunction (%d/%d) emitting URL %s to fetch", _partition, _parallelism, fetchUrl));
					collector.collect(fetchUrl);
				} else {
					needMerge = true;
					break;
				}
			}
		} else if (url.getUrlType() == UrlType.TERMINATION) {
			// TODO flush queue, set flag
		} else {
			throw new RuntimeException("Unknown URL type: " + url.getUrlType());
		}
		
		if (needMerge && _addSinceMerge.get()) {
			// We don't have any active URLs left.
			// Call the CrawlDB to trigger a merge.
			LOGGER.debug(String.format("CrawlDBFunction (%d/%d) merging crawlDB ", _partition, _parallelism));
			_crawlDB.merge();
			_addSinceMerge.set(false);
		}
	}

}
