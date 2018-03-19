package com.scaleunlimited.flinkcrawler.fetcher;

import org.apache.http.HttpStatus;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.util.Headers;

/**
 * A builder that returns a do-nothing (URL always not found) BaseHttpFetcher.
 *
 */
@SuppressWarnings("serial")
public class NoopHttpFetcherBuilder extends BaseHttpFetcherBuilder {

    public NoopHttpFetcherBuilder(UserAgent userAgent) {
        super(1, userAgent);
    }

    @Override
    public BaseHttpFetcher build() throws Exception {

        return new BaseHttpFetcher(0, getUserAgent()) {

            @Override
            public FetchedResult get(String robotsUrl, Payload payload) throws BaseFetchException {
                final int responseRate = 1000;
                return new FetchedResult(robotsUrl, robotsUrl, 0, new Headers(), new byte[0],
                        "text/plain", responseRate, payload, robotsUrl, 0, "192.168.1.1",
                        HttpStatus.SC_NOT_FOUND, null);
            }

            @Override
            public void abort() {
            }
        };
    }
}
