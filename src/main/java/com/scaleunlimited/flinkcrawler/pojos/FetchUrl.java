package com.scaleunlimited.flinkcrawler.pojos;


@SuppressWarnings("serial")
public class FetchUrl extends ScoredUrl {

	private long _crawlDelay;
	
	public FetchUrl(ValidUrl url) {
		super(url, 0.0f);
	}
	
	public FetchUrl(ValidUrl url, float score) {
		super(url, score);
		
		// TODO fill in the additional fields from the crawlrec so that we have them downstream
	}

	public void setCrawlDelay(long crawlDelay) {
		_crawlDelay = crawlDelay;
	}
	
	public long getCrawlDelay() {
		return _crawlDelay;
	}
}
