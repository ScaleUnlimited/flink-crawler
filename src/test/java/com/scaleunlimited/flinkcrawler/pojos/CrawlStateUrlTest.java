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

}
