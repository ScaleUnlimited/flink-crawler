package com.scaleunlimited.flinkcrawler.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

public class FetchQueueTest {

    @Test
    public void testPriorityQueue() throws Exception {
        FetchQueue queue = new FetchQueue(2);
        queue.open();

        CrawlStateUrl url1 = new CrawlStateUrl(new RawUrl("http://domain.com/page1"));
        url1.setScore(1.0f);
        assertNull(queue.add(url1));

        CrawlStateUrl url2 = new CrawlStateUrl(new RawUrl("http://domain.com/page2"));
        url2.setScore(1.5f);
        assertNull(queue.add(url2));
        
        // We should get the second URL as our first one to fetch, since it has a
        // higher score.
        assertSameUrl(url2, queue.poll());
        
        // Add it back.
        assertNull(queue.add(url2));

        CrawlStateUrl url3 = new CrawlStateUrl(new RawUrl("http://domain.com/page3"));
        url3.setScore(0.5f);
        assertTrue(url3 == queue.add(url3));
        
        // Add a higher-scoring URL. We should get back the lowest-scoring URL in the
        // fetch queue.
        CrawlStateUrl url4 = new CrawlStateUrl(new RawUrl("http://domain.com/page4"));
        url4.setScore(2.0f);
        assertSameUrl(url1, queue.add(url4));

        // Verify we get URLs from the queue in priority order.
        assertSameUrl(url4, queue.poll());
        assertSameUrl(url2, queue.poll());
        assertNull(queue.poll());
        assertTrue(queue.isEmpty());
    }
    
    private void assertSameUrl(CrawlStateUrl a, CrawlStateUrl b) {
        assertNotNull(a);
        assertNotNull(b);
        assertEquals(a.getUrl(), b.getUrl());
    }

    @Test
    public void testRejectionByStatus() throws Exception {
        FetchQueue queue = new FetchQueue(2);
        queue.open();

        CrawlStateUrl url1 = new CrawlStateUrl(new RawUrl("http://domain.com/page1"));
        url1.setScore(1.0f);
        url1.setStatus(FetchStatus.UNFETCHED);
        assertNull(queue.add(url1));

        CrawlStateUrl url2 = new CrawlStateUrl(new RawUrl("http://domain.com/page2"));
        url2.setScore(1.0f);
        url2.setStatus(FetchStatus.FETCHED);
        assertEquals(url2, queue.add(url2));
    }
    
    @Test
    public void testRoundTrip() throws Exception {
        FetchQueue queue = new FetchQueue(100);
        queue.open();

        final String urlAsString = "https://apachebigdata2017.sched.com/overview/type/BoFs";
        CrawlStateUrl url = new CrawlStateUrl(new RawUrl(urlAsString));
        queue.add(url);

        CrawlStateUrl urlToFetch = queue.poll();
        assertNotNull(urlToFetch);
        assertEquals(urlAsString, urlToFetch.getUrl());
    }

}
