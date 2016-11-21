package com.scaleunlimited.flinkcrawler.functions;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.config.FetchPolicy;
import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

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
 * TODO - Use the Timelyxxx function approach, versus the CoFlatMap with a tickler approach?
 *
 */
@SuppressWarnings("serial")
public class CrawlDBFunction extends RichCoFlatMapFunction<CrawlStateUrl, Tuple0, FetchUrl> {

	// TODO configure this
	private static final int MAX_ACTIVE_URLS = 10_000;
	
	private BaseCrawlDB _crawlDB;
	
	// List of URLs that are available to be fetched.
	private transient ConcurrentLinkedQueue<FetchUrl> _active;

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

		_active = new ConcurrentLinkedQueue<>();
		
		// TODO set fetch policy via our constructor
		_crawlDB.open(new FetchPolicy(), _active, MAX_ACTIVE_URLS);
	}
	
	@Override
	public void close() throws Exception {
		_crawlDB.close();
		super.close();
	}

	@Override
	public void flatMap1(CrawlStateUrl url, Collector<FetchUrl> collector) throws Exception {
		System.out.println("CrawlDBFunction got " + url);
		
		// Add to our in-memory queue. If this is full, it might trigger a merge
		_crawlDB.add(url);
	}

	/* We get called regularly by a broadcast from the CrawlDbSource, which exists to generate
	 * new URLs as needed from the crawlDB.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.flink.streaming.api.functions.co.CoFlatMapFunction#flatMap2(java.lang.Object, org.apache.flink.util.Collector)
	 */
	@Override
	public void flatMap2(Tuple0 tickle, Collector<FetchUrl> collector) throws Exception {
		// TODO do this in a loop?
		FetchUrl url = _active.poll();
		
		if (url != null) {
			System.out.format("CrawlDBFunction emitting URL %s to fetch (partition %d of %d)\n", url, _index, _parallelism);
			collector.collect(url);
		} else {
			// We don't have any active URLs to fetch.
			// Call the CrawlDB to trigger a merge (if appropriate)
			_crawlDB.merge();
		}
	}
	
	

}
