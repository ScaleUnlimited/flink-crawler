package com.scaleunlimited.flinkcrawler.pojos;

import static org.junit.Assert.*;

import org.junit.Test;

public class CrawlStateUrlTest {

	@Test
	public void testToAndFromByteArray() throws Exception {
		ValidUrl url = new ValidUrl("http://domain.com?q=s");
		CrawlStateUrl csu = new CrawlStateUrl(url, FetchStatus.FETCHED, 100, 1.0f, 1000);
		
		byte[] value = new byte[CrawlStateUrl.VALUE_SIZE];
		csu.getValue(value);
		
		CrawlStateUrl csu2 = new CrawlStateUrl(url, FetchStatus.UNFETCHED, 0, 0.0f, 0);
		csu2.setFromValue(value);
	    assertEquals(csu.getUrl(), csu2.getUrl());
	}

}
