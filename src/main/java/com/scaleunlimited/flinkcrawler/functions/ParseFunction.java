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
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.DomainScore;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;

@SuppressWarnings("serial")
public class ParseFunction extends BaseProcessFunction<FetchResultUrl, ParsedUrl> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseFunction.class);

    public static final OutputTag<CrawlStateUrl> STATUS_OUTPUT_TAG 
        = new OutputTag<CrawlStateUrl>("status"){};
    public static final OutputTag<ExtractedUrl> OUTLINK_OUTPUT_TAG 
        = new OutputTag<ExtractedUrl>("outlink"){};
    public static final OutputTag<DomainScore> SCORE_OUTPUT_TAG
        = new OutputTag<DomainScore>("score"){};
    
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
    public void processElement( FetchResultUrl fetchResultUrl,
                                Context context,
                                Collector<ParsedUrl> collector) 
        throws Exception {

        // We need to update the URL's status, but if it wasn't a successful fetch then we have nothing to parse
        context.output(STATUS_OUTPUT_TAG, new CrawlStateUrl(fetchResultUrl));
        if (fetchResultUrl.getStatus() != FetchStatus.FETCHED) {
            LOGGER.trace("Forwarded failed URL to update status: '{}'", fetchResultUrl.getFetchedUrl());
            return;
        }
        
        // TODO I moved this down here, but would it be better to expect that ParseFunction logs unsuccessful fetches?
        record(this.getClass(), fetchResultUrl);

        ParserResult result;
        try {
            long start = System.currentTimeMillis();
            result = _parser.parse(fetchResultUrl);
            LOGGER.debug("Parsed '{}' in {}ms", fetchResultUrl, System.currentTimeMillis() - start);
        } catch (InterruptedException e) {
            // Ignore, as these happen when we're parsing a file and the workflow
            // is shutting down.
            LOGGER.debug("Interrupted while parsing '{}'", fetchResultUrl);
            return;
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.warn("Parsing exception " + fetchResultUrl, e);
            } else {
                // If we're not doing debug level logging, don't spit out stack trace.
                LOGGER.warn("Parsing exception '{}': {}", fetchResultUrl, e.getCause().getMessage());
            }

            return;
        }

        // Output the content only if we have a score that is greater than 0
        if (result.getParsedUrl().getScore() > 0) {
            collector.collect(result.getParsedUrl());
        } else {
            LOGGER.debug("Skipping content output of zero-score '{}'", fetchResultUrl);
        }

        // Output score for the page.
        context.output(SCORE_OUTPUT_TAG, new DomainScore(result.getParsedUrl().getPld(), result.getParsedUrl().getScore()));
        
        // Since we are limiting the number of outlinks, first sort by score and then limit.
        ExtractedUrl[] extractedUrls = result.getExtractedUrls();
        Arrays.sort(extractedUrls, new Comparator<ExtractedUrl>() {

            @Override
            public int compare(ExtractedUrl o1, ExtractedUrl o2) {
                return (int) (o2.getScore() - o1.getScore());
            }
        });
        
        int count = 0;
        boolean tracing = LOGGER.isTraceEnabled();
        for (ExtractedUrl outlink : extractedUrls) {
            if (tracing) {
                LOGGER.trace("Extracted '{}' from '{}'", outlink.getUrl(), fetchResultUrl.getUrl());
            }
            
            context.output(OUTLINK_OUTPUT_TAG, outlink);
            count++;
            if (count >= _maxOutlinksPerPage) {
                break;
            }
        }
    }
}
