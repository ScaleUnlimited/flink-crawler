package com.scaleunlimited.flinkcrawler.functions;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.utils.DrumMap;

/**
 * The Flink operator that wraps our "crawl DB". Incoming URLs are deduplicated and merged in memory.
 * When the in-memory structure is too full, or we need to generate more URLs to fetch, then we merge
 * with an on-disk version; during that process we archive low-scoring URLs and add the highest-scoring
 * URLs to the fetch queue.
 * 
 * We use a custom trigger function that will "trigger" an aggregation (and thus give us a chance
 * to emit tuples) regularly..
 * 
 * TODO this needs to be a Windowing function
 *
 */
@SuppressWarnings("serial")
public class CrawlDBFunction implements FlatMapFunction<CrawlStateUrl, FetchUrl> {

	DrumMap _crawlDB;
	PriorityQueue<FetchUrl> _urls;
	
	public CrawlDBFunction() throws IOException {
		_crawlDB = new DrumMap(10000);
	}

	@Override
	public void flatMap(CrawlStateUrl url, Collector<FetchUrl> collector) throws Exception {
		_crawlDB.add(url.makeKey(), url.makeValue(), url.makePayload());
		
		
		// TODO Auto-generated method stub
		
	}

}
