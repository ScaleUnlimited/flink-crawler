package com.scaleunlimited.flinkcrawler.functions;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.MalformedURLException;

import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import com.scaleunlimited.flinkcrawler.parser.BasePageParser;
import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;

public class ParseFunctionTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testMaxOutlinksPerPage() throws MalformedURLException, Exception {

        // NOTE : In the case of rich functions we would also need to handle
        // parallelism in the open method.
        BasePageParser basePageParser = mock(BasePageParser.class);
        ParseFunction func = new ParseFunction(basePageParser, 2); // Limit to 2 outlinks per page
        Collector<ParsedUrl> parsedUrlCollector = mock(Collector.class);
        ParseFunction.Context parserContext = mock(Context.class);
        FetchResultUrl fetchResultUrl = new FetchResultUrl();
        fetchResultUrl.setFetchedUrl("http://foo.com");
        fetchResultUrl.setStatus(FetchStatus.FETCHED);
        fetchResultUrl.setNextFetchTime(System.currentTimeMillis() + 1000L);

        ParserResult parserResult = mock(ParserResult.class);
        when(basePageParser.parse(fetchResultUrl)).thenReturn(parserResult);
        ParsedUrl parsedUrl = mock(ParsedUrl.class);
        when(parserResult.getParsedUrl()).thenReturn(parsedUrl);
        when(parsedUrl.getScore()).thenReturn(0f); // we don't care about returning the ParsedUrl

        // We have 3 extracted links
        ExtractedUrl[] extractedUrls = {
                new ExtractedUrl("url1", null, null, 1), new ExtractedUrl("url2", null, null, 2),
                new ExtractedUrl("url3", null, null, 3)
        };
        when(parserResult.getExtractedUrls()).thenReturn(extractedUrls);

        when(parsedUrl.getParsedText()).thenReturn("");
        func.processElement(fetchResultUrl, parserContext, parsedUrlCollector);

        verify(parserContext, times(1)).output( eq(ParseFunction.STATUS_OUTPUT_TAG), 
                                                argThat(new MatchCrawlStateUrl(fetchResultUrl)));
        
        // Verify that we only get the top 2 links
        verify(parserContext, times(2)).output( eq(ParseFunction.OUTLINK_OUTPUT_TAG), 
                                                argThat(new MatchExtractedUrls(2)));
    }
    
    private static class MatchCrawlStateUrl
            implements ArgumentMatcher<CrawlStateUrl> {

        FetchResultUrl _fetchResultUrl;
        
        public MatchCrawlStateUrl(FetchResultUrl fetchResultUrl) {
            super();
            _fetchResultUrl = fetchResultUrl;
        }

        @Override
        public boolean matches(CrawlStateUrl crawlStateUrl) {
            return (    crawlStateUrl.getStatus().equals(_fetchResultUrl.getStatus())
                    &&  crawlStateUrl.getNextFetchTime() == _fetchResultUrl.getNextFetchTime());
        }
        
    }

    private static class MatchExtractedUrls
            implements ArgumentMatcher<ExtractedUrl> {

        private float _minScore;

        public MatchExtractedUrls(float minScore) {
            _minScore = minScore;
        }

        @Override
        public boolean matches(ExtractedUrl extractedUrl) {
            if (extractedUrl.getScore() < _minScore) {
                return false;
            }
            return true;
        }
    }

}
