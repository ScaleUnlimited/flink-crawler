package com.scaleunlimited.flinkcrawler.pojos;

public enum UrlType {

	// A "raw" URL has to be validated still.
	RAW,
	
	// A "validated" URL has been run through the lengthening/normalization process.
	VALIDATED,
	
	// A special URL that is used to keep the iteration running smoothly.
	TICKLER,
	
	// A special URL that tells the CrawlDBFunction to stop emitting URLs.
	TERMINATION
}
