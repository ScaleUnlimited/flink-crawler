package com.scaleunlimited.flinkcrawler.fetcher;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.apache.tika.metadata.Metadata;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

@SuppressWarnings("serial")
public class MockRobotsFetcher extends BaseHttpFetcher {

	private Map<String, String> _robotPages;
	
	public MockRobotsFetcher() {
		this(new HashMap<String, String>());
	}
	
	public MockRobotsFetcher(Map<String, String> robotPages) {
		super(1, new UserAgent("mock-robots-fetcher", "user@domain.com", "http://domain.com"));
		_robotPages = robotPages;
	}
	
	@Override
	public FetchedResult get(String robotsUrl, Payload payload) throws BaseFetchException {
		String page = _robotPages.get(robotsUrl);
		
		final int responseRate = 1000;
		
		if (page == null) {
			return new FetchedResult(	robotsUrl, 
										robotsUrl, 
										0, 
										new Metadata(), 
										new byte[0], 
										"text/plain", 
										responseRate, 
										payload, 
										robotsUrl, 
										0, 
										"192.168.1.1", 
										HttpStatus.SC_NOT_FOUND, 
										null);
		} else {
			return new FetchedResult(	robotsUrl,
										robotsUrl, 
										System.currentTimeMillis(), 
										new Metadata(), 
										page.getBytes(StandardCharsets.UTF_8), 
										"text/plain", 
										responseRate, 
										payload, 
										robotsUrl, 
										0, 
										"192.168.1.1", 
										HttpStatus.SC_OK, 
										null);
		}
	}

	@Override
	public void abort() { }

}
