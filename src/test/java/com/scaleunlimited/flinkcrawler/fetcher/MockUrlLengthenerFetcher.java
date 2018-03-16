package com.scaleunlimited.flinkcrawler.fetcher;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpStatus;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.util.Headers;

@SuppressWarnings("serial")
public class MockUrlLengthenerFetcher extends BaseHttpFetcher {

    public static class MockUrlLengthenerFetcherBuilder extends BaseHttpFetcherBuilder {
        private MockUrlLengthenerFetcher _fetcher;

        public MockUrlLengthenerFetcherBuilder(MockUrlLengthenerFetcher fetcher) {
            super(fetcher.getMaxThreads(), fetcher.getUserAgent());
            _fetcher = fetcher;
        }

        @Override
        public BaseHttpFetcher build() {
            return _fetcher;
        }
    }
    
    private Map<String, String> _redirections;

    public MockUrlLengthenerFetcher() {
        this(new HashMap<String, String>());
    }
    
    public MockUrlLengthenerFetcher(Map<String, String> redirections) {
        super(1, makeUserAgent());
        _redirections = redirections;
    }

    public static UserAgent makeUserAgent() {
        return new UserAgent("mock-url-lengthener-fetcher", "user@domain.com", "http://domain.com");
    }
    
    @Override
    public FetchedResult get(String originalUrl, Payload payload) throws BaseFetchException {
        
        final int responseRate = 1000;
        final String mimeType = "text/plain";
        
        Headers headers = new Headers();
        int statusCode = HttpStatus.SC_MOVED_PERMANENTLY;
        String redirectedUrl = _redirections.get(originalUrl);
        if (redirectedUrl == null) {
            redirectedUrl = originalUrl;
            statusCode = HttpStatus.SC_NOT_FOUND;
        } else {
            headers.add(Headers.LOCATION, redirectedUrl);
        }
        
        // With max redirects set to 0, we don't get the redirected URL in the "actually fetched"
        // field of the FetchedResult (it's in the headers Location:xxx entry).
        FetchedResult result = new FetchedResult(   originalUrl, 
                                                    originalUrl, 
                                                    0, 
                                                    headers, 
                                                    new byte[0], 
                                                    mimeType, 
                                                    responseRate, 
                                                    payload, 
                                                    originalUrl, 
                                                    0, 
                                                    "192.168.1.1", 
                                                    statusCode, 
                                                    null);
        return result;
    }

    @Override
    public void abort() { }

}
