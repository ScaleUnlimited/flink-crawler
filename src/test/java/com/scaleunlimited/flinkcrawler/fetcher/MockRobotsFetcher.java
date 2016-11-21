package com.scaleunlimited.flinkcrawler.fetcher;

import org.apache.http.HttpStatus;

import com.scaleunlimited.flinkcrawler.config.UserAgent;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public class MockRobotsFetcher extends BaseFetcher {

	public MockRobotsFetcher() {
		super(new UserAgent("mock-robots-fetcher", "user@domain.com", "http://domain.com"));
	}
	
	@Override
	public FetchedResult get(FetchUrl url) throws BaseFetchException {
		String robotsUrl = url.getUrl();
		return new FetchedResult(robotsUrl, robotsUrl, 0, null, null, null, 0, robotsUrl, 0, HttpStatus.SC_NOT_FOUND);
	}

	@Override
	public void abort() { }

}
