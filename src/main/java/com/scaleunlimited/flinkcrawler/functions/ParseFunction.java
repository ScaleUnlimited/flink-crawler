package com.scaleunlimited.flinkcrawler.functions;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.metrics.CrawlerAccumulator;
import com.scaleunlimited.flinkcrawler.parser.BasePageParser;
import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;

@SuppressWarnings("serial")
public class ParseFunction extends BaseProcessFunction<FetchedUrl, ParsedUrl> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseFunction.class);

    public static final OutputTag<ExtractedUrl> OUTLINK_OUTPUT_TAG 
        = new OutputTag<ExtractedUrl>("outlink"){};
    public static final OutputTag<String> CONTENT_OUTPUT_TAG 
        = new OutputTag<String>("content"){};
    
    private static final String TABS_AND_RETURNS_PATTERN = "[\t\r\n]";
    private BasePageParser _parser;
    private int _maxOutlinksPerPage;

    public ParseFunction(BasePageParser parser, int maxOutlinksPerPage) {
        _parser = parser;
        _maxOutlinksPerPage = maxOutlinksPerPage;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext context = getRuntimeContext();
        _parser.open(new CrawlerAccumulator(context));
    }

    @Override
    public void processElement( FetchedUrl fetchedUrl,
                                Context context,
                                Collector<ParsedUrl> collector) 
        throws Exception {
        record(this.getClass(), fetchedUrl);

        ParserResult result;
        try {
            long start = System.currentTimeMillis();
            result = _parser.parse(fetchedUrl);
            LOGGER.debug(String.format("Parsed '%s' in %dms", fetchedUrl,
                    System.currentTimeMillis() - start));
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.warn("Parsing exception " + fetchedUrl, e);
            } else {
                // If we're not doing debug level logging, don't spit out stack trace.
                LOGGER.warn(String.format("Parsing exception '%s': %s", fetchedUrl,
                        e.getCause().getMessage()));
            }

            return;
        }

        // Output the content only if we have a score that is greater than 0
        if (result.getParsedUrl().getScore() > 0) {
            collector.collect(result.getParsedUrl());
        }

        // Since we are limiting the number of outlinks, first sort by score and then limit.
        ExtractedUrl[] extractedUrls = result.getExtractedUrls();
        Arrays.sort(extractedUrls, new Comparator<ExtractedUrl>() {

            @Override
            public int compare(ExtractedUrl o1, ExtractedUrl o2) {
                return (int) (o2.getScore() - o1.getScore());
            }
        });
        int count = 0;
        for (ExtractedUrl outlink : extractedUrls) {
            String message = String.format("Extracted '%s' from '%s'", outlink.getUrl(),
                    fetchedUrl.getUrl());
            LOGGER.trace(message);
            context.output(OUTLINK_OUTPUT_TAG, outlink);
            count++;
            if (count >= _maxOutlinksPerPage) {
                break;
            }
        }

        // Output the text version of the content
        String contentText = makeContentText(result);
        context.output(CONTENT_OUTPUT_TAG, contentText);
    }

    private String makeContentText(ParserResult result) {
        String parsedText = result.getParsedUrl().getParsedText();
        String contentField = parsedText.replaceAll(TABS_AND_RETURNS_PATTERN, " ");
        return result.getParsedUrl().getUrl() + "\t" + contentField;
    }

}
