package com.scaleunlimited.flinkcrawler.functions;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.crawldb.DrumMap;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

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

	DrumMap _crawlDB;
	PriorityQueue<FetchUrl> _urls;
	
	public CrawlDBFunction() {
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_crawlDB = new DrumMap(10000);
		
		_urls = new PriorityQueue<>();
	}
	
	@Override
	public void close() throws Exception {
		// TODO close the crawl DB
		super.close();
	}

	@Override
	public void flatMap1(CrawlStateUrl url, Collector<FetchUrl> collector) throws Exception {
		// TODO do we worry about async nature of flatMap2 vs. flatMap1 calls?
		if (url == null) {
			System.out.println("Got null CrawlStateUrl");
		} else {
			_crawlDB.add(url.makeKey(), url.makeValue(), url.makePayload());
		}
	}

	/* We get called regularly by a broadcast from the CrawlDbSource, which exists to generate
	 * new URLs as needed from the crawlDB.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.flink.streaming.api.functions.co.CoFlatMapFunction#flatMap2(java.lang.Object, org.apache.flink.util.Collector)
	 */
	@Override
	public void flatMap2(Tuple0 tickle, Collector<FetchUrl> collector) throws Exception {
		if (_urls.isEmpty()) {
			// TODO trigger a merge, so that we re-fill this. If there's nothing written to disk
			// yet, we could just pull from the in-memory queue (no merge needed).
			
			// FUTURE if URLs is getting low do this, so we don't stall out during generation.
		} else {
			// TODO output a batch at a time?
			collector.collect(_urls.remove());
		}
		
	}
	
	

}
