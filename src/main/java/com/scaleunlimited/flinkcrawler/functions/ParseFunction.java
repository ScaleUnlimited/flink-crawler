package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.Outlink;
import com.scaleunlimited.flinkcrawler.pojos.ParsedContent;

@SuppressWarnings("serial")
public class ParseFunction implements FlatMapFunction<FetchedUrl, Tuple2<Outlink, ParsedContent>> {

	@Override
	public void flatMap(FetchedUrl fetchedUrl, Collector<Tuple2<Outlink, ParsedContent>> collector) throws Exception {
		// Output the content.
		collector.collect(new Tuple2<Outlink, ParsedContent>(null, new ParsedContent("hello, world")));
		
		// Output the links
		collector.collect(new Tuple2<Outlink, ParsedContent>(new Outlink("http://domain.com/hello", "hello", null), null));
		collector.collect(new Tuple2<Outlink, ParsedContent>(new Outlink("http://domain.com/world", "world", null), null));
	}

}
