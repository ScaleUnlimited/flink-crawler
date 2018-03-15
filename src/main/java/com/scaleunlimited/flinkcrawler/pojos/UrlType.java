package com.scaleunlimited.flinkcrawler.pojos;

public enum UrlType {

	// A "validated" URL has been run through the lengthening/normalization process.
	REGULAR,
	
	// A special URL that is used to keep the iteration running smoothly.
	TICKLER,
	
	// A special URL that is tells the crawl DB what keyed state to process.
	DOMAIN,
	
	// A special URL that tells the UrlDBFunction to stop emitting URLs.
	TERMINATION
}
