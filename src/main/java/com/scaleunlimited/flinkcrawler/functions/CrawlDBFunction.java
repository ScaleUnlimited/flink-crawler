package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

/**
 * The Flink operator that wraps our "crawl DB". Incoming URLs are deduplicated and merged in memory.
 * When the in-memory structure is too full, or we need to generate more URLs to fetch, then we merge
 * with an on-disk version; during that process we archive low-scoring URLs and add the highest-scoring
 * URLs to the fetch queue.
 * 
 * We take as input two streams - one is fed by the SeedUrlSource, plus a loop-back Iteration, and this
 * contains actual CrawlStateUrl tuples. The other stream is a timed "trigger" stream that regularly
 * generates an ignorable Tuple that we .
 * 
 * TODO this needs to be a Windowing function
 * 
 * Use ContinuousProcessingTimeTrigger
 *
 */
@SuppressWarnings("serial")
public class CrawlDBFunction extends RichCoFlatMapFunction<CrawlStateUrl, Tuple0, FetchUrl> {

	private BaseCrawlDB _crawlDB;
	
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

		_crawlDB.open();
	}
	
	@Override
	public void close() throws Exception {
		_crawlDB.close();
		super.close();
	}

	@Override
	public void flatMap1(CrawlStateUrl url, Collector<FetchUrl> collector) throws Exception {
		System.out.println("CrawlDBFunction got " + url);
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
		FetchUrl url = _crawlDB.get();
		
		if (url != null) {
			System.out.format("CrawlDBFunction emitting URL %s to fetch (partition %d of %d)\n", url, _index, _parallelism);
			collector.collect(url);
		}
	}
	
	

}
