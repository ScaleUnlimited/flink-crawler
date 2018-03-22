package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.http.HttpStatus;
import org.junit.Ignore;
import org.junit.Test;

import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.parser.SimplePageParser;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;

import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

public class CommonCrawlFetcherIT {

    private static final String CACHE_DIR = "./target/commoncrawlcache/";

    @Test
    public void testSingleUrlFetchWithRedirects() throws Exception {
        BaseHttpFetcher fetcher = makeFetcher(1);

        FetchedResult result = fetcher.get("http://cloudera.com/");

        // We've got one redirect from http => https, and another from https://cloudera.com
        // to https://www.cloudera.com
        assertEquals(2, result.getNumRedirects());
        assertEquals(200, result.getStatusCode());

        SimplePageParser parser = new SimplePageParser();
        ParserResult parseResult = parser.parse(new FetchResultUrl(new ValidUrl(result.getBaseUrl()),
                FetchStatus.FETCHED, result.getFetchTime(), result.getFetchedUrl(), result
                        .getHeaders(), result.getContent(), result.getContentType(), result
                        .getResponseRate()));

        ExtractedUrl[] outlinks = parseResult.getExtractedUrls();
        assertTrue(outlinks.length > 0);
    }

    @Ignore
    @Test
    // Enable this test to try pulling a particular URL out of the common crawl dataset.
    public void testSpecificUrl() throws Exception {
        BaseHttpFetcher fetcher = makeFetcher(1);

        FetchedResult result = fetcher.get("http://www.ghandalf.org/actividades/");

        System.out.println("Redirects: " + result.getNumRedirects());
        System.out.println("Status: " + result.getStatusCode());
    }

    @Test
    public void testTwitter() throws Exception {
        BaseHttpFetcher fetcher = makeFetcher(1);

        FetchedResult result = fetcher.get("http://www.twitter.com/");

        // Redirects to https://www.twitter.com/, which isn't in the index.
        assertEquals(HttpStatus.SC_NOT_FOUND, result.getStatusCode());
        assertEquals(1, result.getNumRedirects());
    }

    @Test
    public void testMultiThreading() throws Exception {
        BaseHttpFetcher fetcher = makeFetcher(3);

        final String[] urlsToFetch = normalize("http://cloudera.com/", "http://cnn.com/",
                "http://uspto.gov/", "http://www.google.com/", "http://www.scaleunlimited.com/",
                "http://www.linkedin.com/", "http://www.pinterest.com/",
                "http://www.instagram.com/");

        final Thread[] threads = new Thread[urlsToFetch.length];
        final FetchedResult[] results = new FetchedResult[urlsToFetch.length];

        for (int i = 0; i < urlsToFetch.length; i++) {
            final int index = i;
            threads[i] = new Thread(new Runnable() {

                @Override
                public void run() {
                    String url = urlsToFetch[index];
                    try {
                        results[index] = fetcher.get(url);
                    } catch (Exception e) {
                        System.out.println("Exception fetching url: " + e.getMessage());
                    }
                }
            });
        }

        // Start them all at the same time.
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        boolean allDone = false;
        while (!allDone) {
            Thread.sleep(100L);

            allDone = true;
            for (int i = 0; i < results.length; i++) {
                if (threads[i].isAlive()) {
                    allDone = false;
                    break;
                }
            }
        }

        for (int i = 0; i < results.length; i++) {
            FetchedResult result = results[i];
            assertNotNull("Failed to fetch URL " + urlsToFetch[i], result);
            assertEquals("Error fetching " + result.getBaseUrl(), HttpStatus.SC_OK,
                    result.getStatusCode());
        }
    }

    private String[] normalize(String... urls) {
        SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();

        String[] result = new String[urls.length];
        for (int i = 0; i < urls.length; i++) {
            result[i] = normalizer.normalize(urls[i]);
        }

        return result;
    }

    private BaseHttpFetcher makeFetcher(int numThreads) throws Exception {
        CommonCrawlFetcherBuilder builder = new CommonCrawlFetcherBuilder(numThreads,
                new UserAgent("", "", ""), "2017-17", CACHE_DIR);
        BaseHttpFetcher fetcher = builder.build();
        return fetcher;
    }
}
