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
public class ParseFunction extends RichFlatMapFunction<FetchedUrl, Tuple3<ExtractedUrl, ParsedUrl, String>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseFunction.class);

    private static final String TABS_AND_RETURNS_PATTERN = "[\t\r\n]";
	private BasePageParser _parser;

	public ParseFunction(BasePageParser parser) {
        _parser = parser;
    }

	@Override
	public void flatMap(FetchedUrl fetchedUrl, Collector<Tuple3<ExtractedUrl, ParsedUrl, String>> collector) throws Exception {
			
		UrlLogger.record(this.getClass(), fetchedUrl);
		
		ParserResult result;
		try {
			result = _parser.parse(fetchedUrl);
		} catch (Exception e) {
			LOGGER.warn("Parsing exception", e);
			return;
		}
		
		// Output the content
		collector.collect(new Tuple3<ExtractedUrl, ParsedUrl, String>(null, result.getParsedUrl(), null));
		
		// Output the links
		for (ExtractedUrl outlink : result.getExtractedUrls()) {
			String message =
				String.format(	"Extracted '%s' from '%s'",
								outlink.getUrl(),
								fetchedUrl.getUrl());
			LOGGER.debug(message);
			collector.collect(new Tuple3<ExtractedUrl, ParsedUrl, String>(outlink, null, null));
		}
		
		// Output the text version of the content
		String contentText = makeContentText(result);
		collector.collect(new Tuple3<ExtractedUrl, ParsedUrl, String>(null, null, contentText));
	}

	private String makeContentText(ParserResult result) {
		String parsedText = result.getParsedUrl().getParsedText();
		String contentField = parsedText.replaceAll(TABS_AND_RETURNS_PATTERN, " ");
		return result.getParsedUrl().getUrl() + "\t" + contentField;
	}

}
