package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.MapFunction;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class RawToStateUrlFunction implements MapFunction<RawUrl, CrawlStateUrl> {

	@Override
	public CrawlStateUrl map(RawUrl url) throws Exception {
		// System.out.println("Converting raw url to crawl state url " + url.getUrl());
		// TODO do real mapping here
		return new CrawlStateUrl(url.getUrl(), FetchStatus.UNFETCHED, "pld", Float.NaN, url.getEstimatedScore(), 0, 0);
	}

}
