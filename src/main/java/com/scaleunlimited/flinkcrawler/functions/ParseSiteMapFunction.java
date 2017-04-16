package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.parser.BasePageParser;
import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings("serial")
public class ParseSiteMapFunction extends RichFlatMapFunction<FetchedUrl, Tuple3<ExtractedUrl, ParsedUrl, String>> {

    static final Logger LOGGER = LoggerFactory.getLogger(ParseSiteMapFunction.class);

	private BasePageParser _siteMapParser;

	public ParseSiteMapFunction(BasePageParser siteMapParser) {
		_siteMapParser = siteMapParser;
    }

	@Override
	public void flatMap(FetchedUrl fetchedUrl, Collector<Tuple3<ExtractedUrl, ParsedUrl, String>> collector) throws Exception {
			
		UrlLogger.record(this.getClass(), fetchedUrl);
		
		try {
			ParserResult parserResult = _siteMapParser.parse(fetchedUrl);
			
			if (parserResult != null) {
				for (ExtractedUrl extractedUrl : parserResult.getExtractedUrls()) {
					collector.collect(new Tuple3<ExtractedUrl, ParsedUrl, String>(extractedUrl, null, null));
				}
			} 
		} catch (Throwable t ) {
			LOGGER.info(String.format("Error while parsing sitemap url %s : %s", fetchedUrl.getFetchedUrl(), t.getMessage()));
		}
	}

}
