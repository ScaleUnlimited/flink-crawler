package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import static org.junit.Assert.*;

import org.apache.http.HttpStatus;
import org.junit.Test;

import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;

import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

public class CommonCrawlFetcherTest {

	private static final String CACHE_DIR = "./target/commoncrawlcache/";

	@Test
	public void testSingleUrlFetchWithRedirects() throws Exception {
		CommonCrawlFetcherBuilder builder = new CommonCrawlFetcherBuilder(1, new UserAgent("", "", ""))
			.setCrawlId("2017-17")
			.setCacheDir(CACHE_DIR)
			.prepCache();
		BaseHttpFetcher fetcher = builder.build();
		
		FetchedResult result = fetcher.get("http://cloudera.com/");
		
		// We've got one redirect from http => https, and another from https://cloudera.com
		// to https://www.cloudera.com
		assertEquals(2, result.getNumRedirects());
		assertEquals(200, result.getStatusCode());
		
		// TODO parse the resulting page, to verify it's valid HTML
		// System.out.format("'%s' (%d): %s\n", result.getBaseUrl(), result.getStatusCode(), new String(result.getContent()));
	}
	
	@Test
	public void testTwitter() throws Exception {
		CommonCrawlFetcherBuilder builder = new CommonCrawlFetcherBuilder(1, new UserAgent("", "", ""))
				.setCrawlId("2017-17")
				.setCacheDir(CACHE_DIR)
				.prepCache();
			BaseHttpFetcher fetcher = builder.build();
		
		FetchedResult result = fetcher.get("http://www.twitter.com/");
		
		// Redirects to https://www.twitter.com/, which isn't in the index.
		assertEquals(HttpStatus.SC_NOT_FOUND, result.getStatusCode());
		assertEquals(1, result.getNumRedirects());
	}
	
	@Test
	public void testMultiThreading() throws Exception {
		final int numThreads = 3;
		CommonCrawlFetcherBuilder builder = new CommonCrawlFetcherBuilder(numThreads, new UserAgent("", "", ""))
				.setCrawlId("2017-22")
				.setCacheDir(CACHE_DIR)
				.prepCache();
		BaseHttpFetcher fetcher = builder.build();
		
		final String[] urlsToFetch = normalize("http://cloudera.com/",
				"http://cnn.com/",
				"http://uspto.gov/",
				"http://www.google.com/",
				"http://www.scaleunlimited.com/",
				"http://www.linkedin.com/",
				"http://www.pinterest.com/",
				"http://www.instagram.com/");
		
		
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
	                } catch (Exception e) {
	                    System.out.println("Exception fetching url: " + e.getMessage());
	                }
	            }
	        });
		}
		
		// Start them all at the same time.
		for (int i = 0; i < threads.length; i++) {
	        threads[i].start();
		}
		
		boolean allDone = false;
		while (!allDone) {
			Thread.sleep(100L);
			
			allDone = true;
			for (int i = 0; i < results.length; i++) {
				if (threads[i].isAlive()) {
					allDone = false;
					break;
				}
			}
		}
		
		for (int i = 0; i < results.length; i++) {
			FetchedResult result = results[i];
			assertNotNull("Failed to fetch URL " + urlsToFetch[i], result);
			assertEquals("Error fetching " + result.getBaseUrl(), HttpStatus.SC_OK, result.getStatusCode());
		}
	}
	
	private String[] normalize(String...urls) {
		SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();

		String[] result = new String[urls.length];
		for (int i = 0; i < urls.length; i++) {
			result[i] = normalizer.normalize(urls[i]);
		}
		
		return result;
	}



}
