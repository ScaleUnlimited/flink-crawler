package com.scaleunlimited.flinkcrawler.crawldb;

import static org.assertj.core.api.Assertions.*;

import org.assertj.core.data.Percentage;
import org.junit.Test;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger.MergeResult;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;

public class DefaultCrawlDBMergerTest {

    @Test
    public void testMergingUnfetched() throws Exception {
        BaseCrawlDBMerger merger = new DefaultCrawlDBMerger();
        
        byte[] firstValue = new byte[CrawlStateUrl.VALUE_SIZE];
        byte[] secondValue = new byte[CrawlStateUrl.VALUE_SIZE];
        byte[] mergedValue = new byte[CrawlStateUrl.VALUE_SIZE];
        
		ValidUrl url = new ValidUrl("http://domain.com?q=s");
		CrawlStateUrl csu1 = new CrawlStateUrl(url, FetchStatus.UNFETCHED, 100, 1.0f, 1000);
		csu1.getValue(firstValue);
		
		CrawlStateUrl csu2 = new CrawlStateUrl(url, FetchStatus.UNFETCHED, 100, 1.0f, 1000);
		csu2.getValue(secondValue);
		
        MergeResult result = merger.doMerge(firstValue, secondValue, mergedValue);
        assertThat(result).isEqualTo(MergeResult.USE_MERGED);
        
		csu1.setFromValue(mergedValue);
		assertThat(csu1.getScore()).isCloseTo(2.0f, Percentage.withPercentage(0.01));
    }

    @Test
    public void testMergingFetchedWithUnfetched() throws Exception {
        BaseCrawlDBMerger merger = new DefaultCrawlDBMerger();
        
        byte[] firstValue = new byte[CrawlStateUrl.VALUE_SIZE];
        byte[] secondValue = new byte[CrawlStateUrl.VALUE_SIZE];
        byte[] mergedValue = new byte[CrawlStateUrl.VALUE_SIZE];
        
		ValidUrl url = new ValidUrl("http://domain.com?q=s");
		CrawlStateUrl csu1 = new CrawlStateUrl(url, FetchStatus.FETCHED, 100, 1.0f, 1000);
		csu1.getValue(firstValue);
		
		CrawlStateUrl csu2 = new CrawlStateUrl(url, FetchStatus.UNFETCHED, 100, 1.0f, 1000);
		csu2.getValue(secondValue);
		
        MergeResult result = merger.doMerge(firstValue, secondValue, mergedValue);
        assertThat(result).isEqualTo(MergeResult.USE_FIRST);
    }

}
