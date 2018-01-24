package com.scaleunlimited.flinkcrawler.fetcher;

import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.SimpleHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

@SuppressWarnings("serial")
public class SimpleHttpFetcherBuilder extends BaseHttpFetcherBuilder {
	
	public static final int DEFAULT_MAX_SIMULTANEOUS_REQUESTS = 1;

	public SimpleHttpFetcherBuilder(UserAgent userAgent) {
		super(DEFAULT_MAX_SIMULTANEOUS_REQUESTS, userAgent);
	}

	public SimpleHttpFetcherBuilder(int maxSimultaneousRequests, UserAgent userAgent) {
		super(maxSimultaneousRequests, userAgent);
	}

	public SimpleHttpFetcherBuilder(int maxThreads, int timeoutInSeconds, UserAgent userAgent) {
		super(maxThreads, userAgent);
		setTimeoutInSeconds(timeoutInSeconds);
	}


	@Override
	public BaseHttpFetcher build() {
		// Note that crawler-commons fetcher uses "maxThreads", but it actually means
		// the size of the connection pool, and thus the max number of simultaneous
		// requests.
		SimpleHttpFetcher result = new SimpleHttpFetcher(_maxThreads, _userAgent);
		return configure(result);
	}

}
