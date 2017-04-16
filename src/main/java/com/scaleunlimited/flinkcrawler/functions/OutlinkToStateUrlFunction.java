package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings("serial")
public class OutlinkToStateUrlFunction implements MapFunction<Tuple3<ExtractedUrl, ParsedUrl, String>, RawUrl> {

	@Override
	public RawUrl map(Tuple3<ExtractedUrl, ParsedUrl, String> outlink) throws Exception {
		ExtractedUrl outlinkUrl = outlink.f0;
		
		UrlLogger.record(this.getClass(), outlinkUrl);
		
		// TODO the extracted url needs to have an estimated score as part of it.
		return new RawUrl(outlinkUrl.getUrl(), 1.0f);
	}

}
