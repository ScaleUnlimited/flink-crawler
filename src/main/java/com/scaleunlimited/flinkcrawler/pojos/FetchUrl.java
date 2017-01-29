package com.scaleunlimited.flinkcrawler.pojos;

import java.net.MalformedURLException;
import java.net.URL;

import crawlercommons.url.PaidLevelDomain;

@SuppressWarnings("serial")
public class FetchUrl extends ScoredUrl {

	private long _crawlDelay;
	
	public FetchUrl(String url) throws MalformedURLException {
		super(url, PaidLevelDomain.getPLD(new URL(url)), 0.0f, 0.0f);
	}
	
	public FetchUrl(String url, String pld, float estimatedScore, float actualScore) {
		super(url, pld, estimatedScore, actualScore);
		
		// TODO fill in the additional fields from the crawlrec so that we have them downstream
	}

	public void setCrawlDelay(long crawlDelay) {
		_crawlDelay = crawlDelay;
	}
	
	public long getCrawlDelay() {
		return _crawlDelay;
	}
}
