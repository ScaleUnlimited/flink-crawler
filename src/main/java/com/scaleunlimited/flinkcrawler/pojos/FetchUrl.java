package com.scaleunlimited.flinkcrawler.pojos;

public class FetchUrl extends ScoredUrl {


	public FetchUrl(String url, float estimatedScore, String pld, float actualScore) {
		super(url, estimatedScore, pld, actualScore);
		
		// TODO fill in the additional fields from the crawlrec so that we have them downstream
	}

	
}
