package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public class CheckUrlWithRobotsFunction extends RichCoFlatMapFunction<FetchUrl, Tuple0, FetchUrl> {

	@Override
	public void flatMap1(FetchUrl url, Collector<FetchUrl> collector) throws Exception {
		// TODO add to queue to be processed
	}

	@Override
	public void flatMap2(Tuple0 tickle, Collector<FetchUrl> collector) throws Exception {
		// TODO output either URL to be fetched, or URL that was blocked.

	}

}
