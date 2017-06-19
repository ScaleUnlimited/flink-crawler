package com.scaleunlimited.flinkcrawler.fetcher;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;

public class CommonCrawlFetcherTest {

	@Test
	public void testSingleUrlFetchWithRedirects() throws Exception {
		BaseHttpFetcher fetcher = new CommonCrawlFetcher("2017-17");
		
		FetchedResult result = fetcher.get("http://cloudera.com/");
		
		// We've got one redirect from http => https, and another from https://cloudera.com
		// to https://www.cloudera.com
		assertEquals(2, result.getNumRedirects());
		assertEquals(200, result.getStatusCode());
		
		// TODO parse the resulting page, to verify it's valid HTML
		System.out.format("'%s' (%d): %s\n", result.getBaseUrl(), result.getStatusCode(), new String(result.getContent()));
	}
	
	@Test
	public void testMultiThreading() throws Exception {
		final int numThreads = 2;
		BaseHttpFetcher fetcher = new CommonCrawlFetcher("2017-22", numThreads, 1 * 1024 * 1024);
		
		// TODO need to deal with URL normalization, since no trailing / means we don't find
		// any of these pages.
		final String[] urlsToFetch = new String[] {
				"http://cloudera.com/",
				"http://cnn.com/",
				"http://google.com/",
				"http://facebook.com/",
				"http://twitter.com/",
				"http://uspto.gov/"
		};
		
		final Thread[] threads = new Thread[urlsToFetch.length];
		final FetchedResult[] results = new FetchedResult[urlsToFetch.length];
		
		for (int i = 0; i < urlsToFetch.length; i++) {
			final int index = i;
			threads[i] = new Thread(new Runnable() {
	            
	            @Override
	            public void run() {
	                String url = urlsToFetch[index];
	                try {
	                	results[index] = fetcher.get(url);
	                } catch (BaseFetchException e) {
	                    System.out.println("Exception fetching url: " + e.getMessage());
	                }
	            }
	        });
		}
		
		// Start them all at the same time.
		for (int i = 0; i < threads.length; i++) {
	        threads[i].start();
		}
		
		// TODO decide how to validate results. Make sure we have all results?
		boolean allDone = false;
		while (!allDone) {
			Thread.sleep(100L);
			
			allDone = true;
			for (int i = 0; i < results.length; i++) {
				if (results[i] == null) {
					allDone = false;
					break;
				}
			}
		}
	}

}
