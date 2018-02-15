package com.scaleunlimited.flinkcrawler.pojos;


@SuppressWarnings("serial")
public class FetchUrl extends ScoredUrl {

	private long _crawlDelay;
	
	public FetchUrl() {
	    super();
	}
	
	public FetchUrl(ValidUrl url) {
		super(url, 0.0f);
	}
	
	public FetchUrl(ValidUrl url, float score) {
		super(url, score);
	}

	public void setCrawlDelay(long crawlDelay) {
		_crawlDelay = crawlDelay;
	}
	
	public long getCrawlDelay() {
		return _crawlDelay;
	}
}
