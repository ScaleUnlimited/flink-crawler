package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class NormalizeUrlsFunction extends RichFlatMapFunction<RawUrl, RawUrl> {

	public NormalizeUrlsFunction() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void flatMap(RawUrl input, Collector<RawUrl> collector) throws Exception {
		// TODO normalize the URL.
		collector.collect(input);
	}

}
