package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.parser.BasePageParser;
import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;

@SuppressWarnings("serial")
public class ParseSiteMapFunction
        extends BaseFlatMapFunction<FetchResultUrl, ExtractedUrl> {

    static final Logger LOGGER = LoggerFactory.getLogger(ParseSiteMapFunction.class);

    private BasePageParser _siteMapParser;

    public ParseSiteMapFunction(BasePageParser siteMapParser) {
        _siteMapParser = siteMapParser;
    }

    @Override
    public void flatMap(FetchResultUrl fetchedUrl,
            Collector<ExtractedUrl> collector) throws Exception {
        record(this.getClass(), fetchedUrl);

        if (fetchedUrl.getStatus() != FetchStatus.FETCHED) {
            LOGGER.trace("Skipping failed site map URL: '{}'", fetchedUrl.getFetchedUrl());
            return;
        }
        
        try {
            ParserResult parserResult = _siteMapParser.parse(fetchedUrl);

            if (parserResult != null) {
                for (ExtractedUrl extractedUrl : parserResult.getExtractedUrls()) {
                    LOGGER.trace("Emitting sitemap URL: {}", extractedUrl);
                    collector.collect(extractedUrl);
                }
            }
        } catch (Throwable t) {
            LOGGER.error(String.format("Error while parsing sitemap url %s: %s",
                    fetchedUrl.getFetchedUrl(), t.getMessage()), t);
        }
    }

}
