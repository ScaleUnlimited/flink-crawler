package com.scaleunlimited.flinkcrawler.fetcher;

import org.apache.http.HttpStatus;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

@SuppressWarnings("serial")
public class MockRobotsFetcher extends BaseHttpFetcher {

	public MockRobotsFetcher() {
		super(1, new UserAgent("mock-robots-fetcher", "user@domain.com", "http://domain.com"));
	}
	
	@Override
	public FetchedResult get(String robotsUrl, Payload payload) throws BaseFetchException {
		return new FetchedResult(robotsUrl, robotsUrl, 0, null, null, null, 0, null, robotsUrl, 0, robotsUrl, HttpStatus.SC_NOT_FOUND, robotsUrl);
	}

	@Override
	public void abort() { }

}
