package com.scaleunlimited.flinkcrawler.pojos;

import static org.junit.Assert.*;

import org.junit.Test;

public class ExtractedUrlTest {

	@Test
	public void testToString() {
		ExtractedUrl url = new ExtractedUrl("http://domain.com", "click me", "", 0.5f);
		assertEquals("[click me](http://domain.com) - 0.50", url.toString());
	}
}
