package com.scaleunlimited.flinkcrawler.fetcher;

import static org.junit.Assert.*;

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

}
