package com.scaleunlimited.flinkcrawler.crawldb;

import static org.junit.Assert.*;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;

public class DrumCrawlDBTest {

	@Test
	public void testMergingAndKeepingState() throws Exception {
		File testDir = new File("target/test/DrumCrawlDBTest/testMergingAndKeepingState");
		if (testDir.exists()) {
			FileUtils.deleteDirectory(testDir);
		}
		
		DrumCrawlDB cdb = new DrumCrawlDB(10, "target/test/DrumCrawlDBTest/testMergingAndKeepingState/");
		FetchQueue fq = new FetchQueue(100);
		cdb.open(0, fq, new DefaultCrawlDBMerger());
		
		CrawlStateUrl url1 = makeUrl("domain1.com/page1", FetchStatus.UNFETCHED);
		cdb.add(url1);
		cdb.merge();
		
		assertFalse(fq.isEmpty());
		FetchUrl fu = fq.poll();
		assertNotNull(fu);
		assertEquals(url1.getUrl(), fu.getUrl());
		assertTrue(fq.isEmpty());
		
		// If we get the same URL again (as an outlink) we shouldn't try to fetch it again,
		// as we're already fetching it.
		CrawlStateUrl url2 = makeUrl("domain1.com/page1", FetchStatus.UNFETCHED);
		cdb.add(url2);
		cdb.merge();
		
		assertTrue(fq.isEmpty());
	}

	private CrawlStateUrl makeUrl(String url, FetchStatus status) throws MalformedURLException {
		ValidUrl vu = new ValidUrl("http://" + url);
		return new CrawlStateUrl(vu, status, System.currentTimeMillis(), 1.0f, System.currentTimeMillis());
	}

}
