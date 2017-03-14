package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;

@SuppressWarnings({ "serial" })
public class HandleFailedSiteMapFunction extends RichFilterFunction<CrawlStateUrl> {
	static final Logger LOGGER = LoggerFactory.getLogger(HandleFailedSiteMapFunction.class);
	
	public HandleFailedSiteMapFunction() {
	}

	@Override
	public boolean filter(CrawlStateUrl crawlStateUrl) throws Exception {
		// only log if failed
		FetchStatus status = crawlStateUrl.getStatus();
		if (status != FetchStatus.FETCHED) {
			LOGGER.info(String.format("Failed fetching sitemap url '%s' due to '%s'", crawlStateUrl.getUrl(), crawlStateUrl.getStatus()));
		}
		return true;
	}
}
