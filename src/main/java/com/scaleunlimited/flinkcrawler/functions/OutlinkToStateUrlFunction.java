package com.scaleunlimited.flinkcrawler.functions;

import java.net.URL;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

import crawlercommons.url.PaidLevelDomain;

@SuppressWarnings("serial")
public class OutlinkToStateUrlFunction implements MapFunction<Tuple2<ExtractedUrl, ParsedUrl>, RawUrl> {

	@Override
	public RawUrl map(Tuple2<ExtractedUrl, ParsedUrl> outlink) throws Exception {
		ExtractedUrl outlinkUrl = outlink.f0;
		
		UrlLogger.record(this.getClass(), outlinkUrl);
		
		// TODO the extracted url needs to have an estimated score as part of it.
		// System.out.println("Converting outlink to raw URL: " + outlinkUrl);
		String url = outlinkUrl.getToUrl();
		String pld = PaidLevelDomain.getPLD(new URL(url));
		return new RawUrl(url, pld, 1.0f);
	}

}
