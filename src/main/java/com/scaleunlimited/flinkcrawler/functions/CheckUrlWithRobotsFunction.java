package com.scaleunlimited.flinkcrawler.functions;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

// TODO give this a BaseFetcher that it uses to fetch robots, parse it, etc.
// Then it can keep track of robot rules, etc here.

@SuppressWarnings("serial")
public class CheckUrlWithRobotsFunction extends RichCoFlatMapFunction<FetchUrl, Tuple0, FetchUrl> {

	private ConcurrentLinkedQueue<FetchUrl> _queue;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_queue = new ConcurrentLinkedQueue<>();
	}
	
	@Override
	public void flatMap1(FetchUrl url, Collector<FetchUrl> collector) throws Exception {
		System.out.println("Checking against robots: " + url);
		_queue.add(url);
	}

	@Override
	public void flatMap2(Tuple0 tickle, Collector<FetchUrl> collector) throws Exception {
		if (!_queue.isEmpty()) {
			FetchUrl url = _queue.remove();
			System.out.println("Url passed robots check: " + url);
			collector.collect(url);
		}

	}

}
