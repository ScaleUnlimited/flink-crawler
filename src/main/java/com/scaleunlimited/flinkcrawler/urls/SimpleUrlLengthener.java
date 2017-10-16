package com.scaleunlimited.flinkcrawler.urls;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class SimpleUrlLengthener extends BaseUrlLengthener {

	@Override
	public RawUrl lengthen(RawUrl url) {
		// If the domain is a link shortener, lengthen it.
		// FUTURE  Try to fetch the URL. This call needs to be thread-safe
		// See https://github.com/ScaleUnlimited/flink-crawler/issues/50
		
		return url;
	}

	@Override
	public int getTimeoutInSeconds() {
		return 10;
	}
}
