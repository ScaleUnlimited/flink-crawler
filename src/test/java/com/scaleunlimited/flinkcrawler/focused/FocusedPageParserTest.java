package com.scaleunlimited.flinkcrawler.focused;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import com.scaleunlimited.flinkcrawler.metrics.CrawlerAccumulator;
import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;

import crawlercommons.util.Headers;

public class FocusedPageParserTest {

	@Test
	public void test() throws Exception {
		TestPageScorer pageScorer = new TestPageScorer();
		FocusedPageParser parser = new FocusedPageParser(pageScorer);
		parser.open(null);
		ValidUrl url = new ValidUrl("http://domain.com/page.html");
		Headers headers = new Headers();
		byte[] content = "<html><head><title></title></head><body><p>0.75</p></body></html>".getBytes(StandardCharsets.UTF_8);
		FetchedUrl fetchedUrl = new FetchedUrl(url, url.getUrl(), 0, headers, content, "text/html", 0);
		ParserResult result = parser.parse(fetchedUrl);
		assertEquals(0.75f, result.getParsedUrl().getScore(), 0.0001f);
	}
	
	@SuppressWarnings("serial")
	private static class TestPageScorer extends BasePageScorer {
		
		@Override
		public float score(ParserResult parse) {
			return Float.parseFloat(parse.getParsedUrl().getParsedText());
		}

		@Override
		public void open(CrawlerAccumulator crawlerAccumulator) throws Exception {
		}

		@Override
		public void close() throws Exception {
		}
	}

}
