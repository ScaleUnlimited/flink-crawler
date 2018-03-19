package com.scaleunlimited.flinkcrawler.tools;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import crawlercommons.fetcher.http.UserAgent;

public class CrawlToolTest {

    private static final UserAgent INVALID_USER_AGENT = new UserAgent(
            "DO NOT USE THIS USER AGENT (i.e., MAKE YOUR OWN)!", "flink-crawler@scaleunlimited.com",
            "https://github.com/ScaleUnlimited/flink-crawler/wiki/Crawler-Policy");

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testOptionsParsing() throws Throwable {
        CrawlToolOptions options = new CrawlToolOptions();
        try {
            options.validate();
        } catch (RuntimeException e) {
            // expected
            assertTrue(e.getMessage().toLowerCase().contains("agent"));
        }

        options.setCommonCrawlId("42");
        options.validate();

        options.setUserAgent(INVALID_USER_AGENT);
        try {
            options.validate();
        } catch (RuntimeException e) {
            // expected
            assertTrue(e.getMessage().toLowerCase().contains("common"));
        }

        options.setCommonCrawlId(null);
        options.validate();
    }

}
