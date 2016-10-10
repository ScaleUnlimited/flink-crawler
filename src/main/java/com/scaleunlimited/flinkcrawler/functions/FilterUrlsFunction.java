package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class FilterUrlsFunction extends RichFlatMapFunction<RawUrl, RawUrl> {

	public FilterUrlsFunction() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void flatMap(RawUrl input, Collector<RawUrl> collector) throws Exception {
		// TODO filter the URL.
		collector.collect(input);
	}

}
