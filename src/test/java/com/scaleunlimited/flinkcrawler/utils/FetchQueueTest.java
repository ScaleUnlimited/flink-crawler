package com.scaleunlimited.flinkcrawler.utils;

import static org.junit.Assert.*;

import org.junit.Test;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

public class FetchQueueTest {

    @Test
    public void testRoundTrip() throws Exception {
        FetchQueue queue = new FetchQueue(100);
        queue.open();
        
        final String urlAsString = "https://apachebigdata2017.sched.com/overview/type/BoFs";
        CrawlStateUrl url = new CrawlStateUrl(new RawUrl(urlAsString));
        queue.add(url);
        
        FetchUrl urlToFetch = queue.poll();
        assertNotNull(urlToFetch);
        assertEquals(urlAsString, urlToFetch.getUrl());
    }

}
