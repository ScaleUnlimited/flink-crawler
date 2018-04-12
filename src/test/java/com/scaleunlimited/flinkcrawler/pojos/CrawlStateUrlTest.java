package com.scaleunlimited.flinkcrawler.pojos;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CrawlStateUrlTest {

    @Test
    public void testSettingFromAnotherUrl() throws Exception {
        ValidUrl url = new ValidUrl("http://domain.com?q=s");
        CrawlStateUrl csu = new CrawlStateUrl(url, FetchStatus.FETCHED, 100);
        csu.setScore(1.0f);
        csu.setNextFetchTime(1000L);

        CrawlStateUrl newUrl = new CrawlStateUrl();
        newUrl.setFrom(csu);

        assertThat(newUrl).isEqualTo(csu);
    }
    
    @Test
    public void testSetFrom() throws Exception {
        ValidUrl url = new ValidUrl("http://domain.com?q=s");
        CrawlStateUrl csu1 = new CrawlStateUrl(url, FetchStatus.UNFETCHED, 100);
        csu1.setStatus(FetchStatus.FETCHING);
        
        CrawlStateUrl csu2 = new CrawlStateUrl();
        csu2.setFrom(csu1);
        assertThat(csu2).isEqualToComparingFieldByField(csu1);
    }
    
    @Test
    public void testPreviousStatus() throws Exception {
        ValidUrl url = new ValidUrl("http://domain.com?q=s");
        CrawlStateUrl csu = new CrawlStateUrl(url, FetchStatus.FETCHING, 100);
        csu.setScore(1.0f);
        csu.setNextFetchTime(1000L);

        csu.setStatus(FetchStatus.FETCHED);
        assertThat(csu.getStatus()).isEqualTo(FetchStatus.FETCHED);
        csu.restorePreviousStatus();
        assertThat(csu.getStatus()).isEqualTo(FetchStatus.FETCHING);

        // If we switch from and then back to a status, make sure it
        // actually does the switch.
        csu.setStatus(FetchStatus.FETCHED);
        csu.setStatus(FetchStatus.FETCHING);
        assertThat(csu.getStatus()).isEqualTo(FetchStatus.FETCHING);
    }

}
