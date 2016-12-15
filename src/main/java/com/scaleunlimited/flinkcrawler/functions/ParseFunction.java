package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.parser.BasePageParser;
import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings("serial")
public class ParseFunction extends RichFlatMapFunction<Tuple2<FetchedUrl, CrawlStateUrl>, Tuple2<ExtractedUrl, ParsedUrl>> {

    private BasePageParser _parser;

	public ParseFunction(BasePageParser parser) {
        _parser = parser;
    }

	@Override
	public void flatMap(Tuple2<FetchedUrl, CrawlStateUrl>fetchTuple2, Collector<Tuple2<ExtractedUrl, ParsedUrl>> collector) throws Exception {
		FetchedUrl fetchedUrl = fetchTuple2.f0;
		if (fetchedUrl != null) {
			
			UrlLogger.record(this.getClass(), fetchedUrl);
			
			ParserResult result = _parser.parse(fetchedUrl);
			
			// Output the content.
			collector.collect(new Tuple2<ExtractedUrl, ParsedUrl>(null, result.getParsedUrl()));
			
			// Output the links
			for (ExtractedUrl outlink : result.getExtractedUrls()) {
				collector.collect(new Tuple2<ExtractedUrl, ParsedUrl>(outlink, null));
			}
		}
	}

}
