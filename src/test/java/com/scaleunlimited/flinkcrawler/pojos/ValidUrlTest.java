package com.scaleunlimited.flinkcrawler.pojos;

import static org.junit.Assert.*;

import org.junit.Test;

public class ValidUrlTest {

	@Test
	public void testUrlWithoutPath() throws Exception {
		ValidUrl url = new ValidUrl("http://domain.com:80/path/to/file?query");
		assertEquals("http://domain.com", url.getUrlWithoutPath());
		
		url = new ValidUrl("http://domain.com:8080");
		assertEquals("http://domain.com:8080", url.getUrlWithoutPath());
		
		url = new ValidUrl("https://domain.com:443");
		assertEquals("https://domain.com", url.getUrlWithoutPath());
		
		url = new ValidUrl("https://domain.com:80");
		assertEquals("https://domain.com:80", url.getUrlWithoutPath());
	}
	
	@Test
	public void testPld() throws Exception {
		ValidUrl url = new ValidUrl("http://wwww.domain.com/path/to/file?query");
		assertEquals("domain.com", url.getPld());
		
		url = new ValidUrl("http://wwww.domain.co.jp/path/to/file?query");
		assertEquals("domain.co.jp", url.getPld());
	}
	
}
