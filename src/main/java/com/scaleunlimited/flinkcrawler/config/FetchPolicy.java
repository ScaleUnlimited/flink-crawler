package com.scaleunlimited.flinkcrawler.config;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;

public class FetchPolicy {
	
	public boolean isFetchable(CrawlStateUrl url) {
		FetchStatus status = url.getStatus();
		
		// TODO make this more sophisticated.
		return status == FetchStatus.UNFETCHED;
	}
}
