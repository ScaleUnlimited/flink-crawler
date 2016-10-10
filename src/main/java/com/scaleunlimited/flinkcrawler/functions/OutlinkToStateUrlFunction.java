package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class OutlinkToStateUrlFunction implements MapFunction<Tuple2<ExtractedUrl, ParsedUrl>, RawUrl> {

	@Override
	public RawUrl map(Tuple2<ExtractedUrl, ParsedUrl> outlink) throws Exception {
		ExtractedUrl outlinkUrl = outlink.f0;
		
		// TODO the extracted url needs to have an estimated score as part of it.
		return new RawUrl(outlinkUrl.getToUrl(), 1.0f);
	}

}
