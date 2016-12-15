package com.scaleunlimited.flinkcrawler.pojos;

@SuppressWarnings("serial")
public class FetchUrl extends ScoredUrl {

	public FetchUrl(String url, String pld, float estimatedScore, float actualScore) {
		super(url, pld, estimatedScore, actualScore);
		
		// TODO fill in the additional fields from the crawlrec so that we have them downstream
	}

	
}
