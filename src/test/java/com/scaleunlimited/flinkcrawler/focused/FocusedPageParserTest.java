package com.scaleunlimited.flinkcrawler.focused;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.Test;
import org.mockito.Mockito;

import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;

import crawlercommons.util.Headers;

public class FocusedPageParserTest {

    @Test
    public void test() throws Exception {
        TestPageScorer pageScorer = new TestPageScorer();
        FocusedPageParser parser = new FocusedPageParser(pageScorer);

        RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
        parser.open(mockContext);
        
        ValidUrl url = new ValidUrl("http://domain.com/page.html");
        Headers headers = new Headers();
        byte[] content = "<html><head><title></title></head><body><p>0.75</p></body></html>"
                .getBytes(StandardCharsets.UTF_8);
        FetchResultUrl fetchedUrl = new FetchResultUrl(url, FetchStatus.FETCHED, 0, url.getUrl(), headers, content, "text/html",
                0);
        ParserResult result = parser.parse(fetchedUrl);
        assertEquals(0.75f, result.getParsedUrl().getScore(), 0.0001f);
    }

    @SuppressWarnings("serial")
    private static class TestPageScorer extends BasePageScorer {

        @Override
        public float score(ParserResult parse) {
            return Float.parseFloat(parse.getParsedUrl().getParsedText());
        }
    }

}
