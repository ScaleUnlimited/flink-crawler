package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import static org.mockito.Mockito.*;

import com.scaleunlimited.flinkcrawler.parser.BasePageParser;
import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;

public class ParseFunctionTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testMaxOutlinksPerPage() throws MalformedURLException, Exception {

        // NOTE : In the case of rich functions we would also need to handle
        // parallelism in the open method.
        BasePageParser basePageParser = mock(BasePageParser.class);
        ParseFunction func = new ParseFunction(basePageParser, 2); // Limit to 2 outlinks per page
        Collector<Tuple3<ExtractedUrl, ParsedUrl, String>> collector = mock(Collector.class);
        FetchedUrl fetchedUrl = new FetchedUrl();
        fetchedUrl.setFetchedUrl("http://foo.com");

        ParserResult parserResult = mock(ParserResult.class);
        when(basePageParser.parse(fetchedUrl)).thenReturn(parserResult);
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
        func.flatMap(fetchedUrl, collector);

        // Verify that we only get the top 2 links
        verify(collector).collect(argThat(new MatchExtractedUrls(2, 2)));
    }

    private static class MatchExtractedUrls
            implements ArgumentMatcher<Tuple3<ExtractedUrl, ParsedUrl, String>> {

        private int _numUrls;
        private float _minScore;
        private int _count = 0;

        public MatchExtractedUrls(int numUrls, float minScore) {
            _numUrls = numUrls;
            _minScore = minScore;
        }

        @Override
        public boolean matches(Tuple3<ExtractedUrl, ParsedUrl, String> tuple3) {
            if ((tuple3.f0 == null) && (tuple3.f1 == null) && (tuple3.f2 != null)) {
                return true; // the content tuple doesn't need to be counted.
            }
            _count++;
            if (_count > _numUrls) {
                return false;
            }
            if (tuple3.f0.getScore() < _minScore) {
                return false;
            }
            return true;
        }
    }

}
