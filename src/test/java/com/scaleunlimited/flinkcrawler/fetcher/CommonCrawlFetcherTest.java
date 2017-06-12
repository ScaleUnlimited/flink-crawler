package com.scaleunlimited.flinkcrawler.fetcher;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;

public class CommonCrawlFetcherTest {

	@Test
	public void test() throws Exception {
		BaseHttpFetcher fetcher = new CommonCrawlFetcher();
		
		// fetcher.get("http://www-cgi.cnn.com/robots.txt");
		FetchedResult result = fetcher.get("http://cloudera.com/");
		System.out.format("'%s' (%d): %s\n", result.getBaseUrl(), result.getStatusCode(), new String(result.getContent()));
	}

	@Test
	public void testRandomSegments() throws Exception {
		CommonCrawlFetcher fetcher = new CommonCrawlFetcher("2017-22", 1, CommonCrawlFetcher.DEFAULT_CACHE_SIZE);
		int totalSegments = fetcher.getNumSegments();
		Random rand = new Random();
		
		for (int i = 0; i < 100; i++) {
			fetcher.loadSegment(rand.nextInt(totalSegments));
		}
	}
}
