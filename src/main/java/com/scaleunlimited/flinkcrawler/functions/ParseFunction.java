package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.parser.BaseParser;
import com.scaleunlimited.flinkcrawler.parser.ParserResult;

@SuppressWarnings("serial")
public class ParseFunction extends RichFlatMapFunction<FetchedUrl, Tuple2<ExtractedUrl, ParsedUrl>> {

    private BaseParser _parser;

	public ParseFunction(BaseParser parser) {
        _parser = parser;
    }

	@Override
	public void flatMap(FetchedUrl fetchedUrl, Collector<Tuple2<ExtractedUrl, ParsedUrl>> collector) throws Exception {
		
		ParserResult result = _parser.parse(fetchedUrl);
		
		// Output the content.
		collector.collect(new Tuple2<ExtractedUrl, ParsedUrl>(null, result.getParsedUrl()));
		
		// Output the links
		for (ExtractedUrl outlink : result.getExtractedUrls()) {
			collector.collect(new Tuple2<ExtractedUrl, ParsedUrl>(outlink, null));
		}
	}

}
