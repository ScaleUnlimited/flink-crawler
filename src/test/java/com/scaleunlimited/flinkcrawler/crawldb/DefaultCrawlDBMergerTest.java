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
        
		ValidUrl url = new ValidUrl("http://domain.com?q=s");
		CrawlStateUrl csu1 = new CrawlStateUrl(url, FetchStatus.UNFETCHED, 100, 1.0f, 1000);
		CrawlStateUrl csu2 = new CrawlStateUrl(url, FetchStatus.UNFETCHED, 100, 1.0f, 1000);
		CrawlStateUrl mergedValue = new CrawlStateUrl();
		
        MergeResult result = merger.doMerge(csu1, csu2, mergedValue);
        assertThat(result).isEqualTo(MergeResult.USE_MERGED);
        assertThat(mergedValue.getUrl()).isEqualTo(url.getUrl());
		assertThat(mergedValue.getScore()).isCloseTo(2.0f, Percentage.withPercentage(0.01));
    }

    @Test
    public void testMergingFetchedWithFetching() throws Exception {
        BaseCrawlDBMerger merger = new DefaultCrawlDBMerger();
        
		ValidUrl url = new ValidUrl("http://domain1.com/");
		CrawlStateUrl stateUrl = new CrawlStateUrl(url, FetchStatus.FETCHING, 100, 1.0f, 1000);
		CrawlStateUrl newUrl = new CrawlStateUrl(url, FetchStatus.FETCHED, 200, 1.0f, 1000);
	    CrawlStateUrl mergedValue = new CrawlStateUrl();

        MergeResult result = merger.doMerge(stateUrl, newUrl, mergedValue);
        assertThat(result).isEqualTo(MergeResult.USE_SECOND);
    }

}
