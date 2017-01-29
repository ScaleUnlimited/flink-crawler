package com.scaleunlimited.flinkcrawler.fetcher;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.apache.tika.metadata.Metadata;

import com.scaleunlimited.flinkcrawler.config.UserAgent;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public class MockRobotsFetcher extends BaseFetcher {

	private Map<String, String> _robotPages;
	
	public MockRobotsFetcher() {
		this(new HashMap<String, String>());
	}
	
	public MockRobotsFetcher(Map<String, String> robotPages) {
		super(new UserAgent("mock-robots-fetcher", "user@domain.com", "http://domain.com"));
		_robotPages = robotPages;
	}
	
	@Override
	public FetchedResult get(FetchUrl url) throws BaseFetchException {
		String robotsUrl = url.getUrl();
		String page = _robotPages.get(robotsUrl);
		if (page == null) {
			// TODO we have to pass in non-null content & type, otherwise FetchedResult will complain...does this make sense?
			return new FetchedResult(robotsUrl, robotsUrl, 0, new Metadata(), new byte[0], "text/plain", 0, robotsUrl, 0, HttpStatus.SC_NOT_FOUND);
		} else {
			return new FetchedResult(robotsUrl, robotsUrl, System.currentTimeMillis(), new Metadata(), page.getBytes(StandardCharsets.UTF_8), "text/plain", 1000, robotsUrl, 0, HttpStatus.SC_OK);
		}
	}

	@Override
	public void abort() { }

}
