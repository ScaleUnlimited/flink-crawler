package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.MapFunction;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class RawToStateUrlFunction implements MapFunction<RawUrl, CrawlStateUrl> {

	@Override
	public CrawlStateUrl map(RawUrl url) throws Exception {
		System.out.println("Converting raw url to crawl state url " + url.getUrl());
		return new CrawlStateUrl(url.getUrl(), "status", "pld", Float.NaN, url.getEstimatedScore(), 0, 0);
	}

}
