package com.scaleunlimited.flinkcrawler.fetcher;

import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.SimpleHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

@SuppressWarnings("serial")
public class SimpleHttpFetcherBuilder extends BaseHttpFetcherBuilder {
	
	public SimpleHttpFetcherBuilder(int maxSimultaneousRequests, UserAgent userAgent) {
		super(maxSimultaneousRequests, userAgent);
	}

	@Override
	public BaseHttpFetcher build() {
		// Note that crawler-commons fetcher uses "maxThreads", but it actually means
		// the size of the connection pool, and thus the max number of simultaneous
		// requests.
		SimpleHttpFetcher result = new SimpleHttpFetcher(_maxSimultaneousRequests, _userAgent);
		return configure(result);
	}

}
