package com.scaleunlimited.flinkcrawler.fetcher;

import static org.junit.Assert.*;

import org.junit.Test;

import crawlercommons.fetcher.http.BaseHttpFetcher;

public class CommonCrawlFetcherTest {

	@Test
	public void test() throws Exception {
		BaseHttpFetcher fetcher = new CommonCrawlFetcher();
		
		fetcher.get("http://www-cgi.cnn.com/robots.txt");
	}

}
