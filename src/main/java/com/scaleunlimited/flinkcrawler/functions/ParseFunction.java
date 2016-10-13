package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;

@SuppressWarnings("serial")
public class ParseFunction extends RichFlatMapFunction<FetchedUrl, Tuple2<ExtractedUrl, ParsedUrl>> {

	@Override
	public void flatMap(FetchedUrl fetchedUrl, Collector<Tuple2<ExtractedUrl, ParsedUrl>> collector) throws Exception {
		// Output the content.
		collector.collect(new Tuple2<ExtractedUrl, ParsedUrl>(null, new ParsedUrl("http://domain.com", "192.168.1.1", "hello, world", "en", "Title", null)));
		
		// Output the links
		collector.collect(new Tuple2<ExtractedUrl, ParsedUrl>(new ExtractedUrl("http://domain.com/hello", "hello", null), null));
		collector.collect(new Tuple2<ExtractedUrl, ParsedUrl>(new ExtractedUrl("http://domain.com/world", "world", null), null));
	}

}
