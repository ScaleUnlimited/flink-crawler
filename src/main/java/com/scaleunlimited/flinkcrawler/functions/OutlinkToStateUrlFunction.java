package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.java.tuple.Tuple3;

import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class OutlinkToStateUrlFunction extends BaseMapFunction<Tuple3<ExtractedUrl, ParsedUrl, String>, RawUrl> {

	@Override
	public RawUrl map(Tuple3<ExtractedUrl, ParsedUrl, String> outlink) throws Exception {
		ExtractedUrl outlinkUrl = outlink.f0;
		record(this.getClass(), outlinkUrl);
		return new RawUrl(outlinkUrl.getUrl(), outlinkUrl.getScore());
	}
}
