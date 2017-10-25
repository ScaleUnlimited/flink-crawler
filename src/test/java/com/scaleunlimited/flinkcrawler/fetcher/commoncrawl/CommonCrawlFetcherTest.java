package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import static org.junit.Assert.*;

import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Test;

public class CommonCrawlFetcherTest {

	@Test
	public void testReverseDomain() throws Exception {
		assertEquals("com,domain)/", reverseIt("http://domain.com"));
		assertEquals("com,domain)/", reverseIt("http://www.domain.com"));
		assertEquals("com,domain)/", reverseIt("https://www.domain.com"));
		assertEquals("com,domain,sub)/", reverseIt("http://sub.domain.com"));
		assertEquals("com,domain:8080)/", reverseIt("http://domain.com:8080"));
		assertEquals("com,domain)/path/to/file", reverseIt("http://domain.com/path/to/file"));
		assertEquals("com,domain)/?q=x", reverseIt("http://domain.com?q=x"));
		assertEquals("com,domain)/path/to/file?q=x", reverseIt("http://domain.com/path/to/file?q=x"));
	}

	private String reverseIt(String url) throws MalformedURLException {
		return CommonCrawlFetcher.reverseDomain(new URL(url));
	}

}
