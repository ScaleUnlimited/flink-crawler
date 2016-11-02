package com.scaleunlimited.flinkcrawler.urls;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class SimpleUrlLengthener extends BaseUrlLengthener {

	@Override
	public RawUrl lengthen(RawUrl url) {
		// If the domain is a link shortener, lengthen it.
		// TODO try to fetch the URL. This call needs to be thread-safe
		return url;
	}

}
