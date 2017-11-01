package com.scaleunlimited.flinkcrawler.fetcher;

import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.SimpleHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

@SuppressWarnings("serial")
public class SimpleHttpFetcherBuilder extends BaseHttpFetcherBuilder {
	
	// SimpleHttpFetcher.DEFAULT_MAX_THREADS should be public, but whatever
	private static final int DEFAULT_MAX_THREADS = 1;

	public SimpleHttpFetcherBuilder(UserAgent userAgent) {
		super(DEFAULT_MAX_THREADS, userAgent);
	}

	public SimpleHttpFetcherBuilder(int maxThreads, UserAgent userAgent) {
		super(maxThreads, userAgent);
	}

	public SimpleHttpFetcherBuilder(int maxThreads, int timeoutInSeconds, UserAgent userAgent) {
		super(maxThreads, userAgent);
		setTimeoutInSeconds(timeoutInSeconds);
	}


	@Override
	public BaseHttpFetcher build() {
		SimpleHttpFetcher result = new SimpleHttpFetcher(_maxThreads, _userAgent);
		return configure(result);
	}

}
