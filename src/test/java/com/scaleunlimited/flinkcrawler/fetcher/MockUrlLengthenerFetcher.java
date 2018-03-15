package com.scaleunlimited.flinkcrawler.fetcher;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpStatus;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.RedirectFetchException;
import crawlercommons.fetcher.RedirectFetchException.RedirectExceptionReason;
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
        String redirectedUrl = _redirections.get(originalUrl);
        
        final int responseRate = 1000;
        
        if (redirectedUrl == null) {
            return new FetchedResult(   originalUrl, 
                                        originalUrl, 
                                        0, 
                                        new Headers(), 
                                        new byte[0], 
                                        "text/plain", 
                                        responseRate, 
                                        payload, 
                                        originalUrl, 
                                        0, 
                                        "192.168.1.1", 
                                        HttpStatus.SC_NOT_FOUND, 
                                        null);
        } else {
            throw new RedirectFetchException(   originalUrl, 
                                                redirectedUrl, 
                                                RedirectExceptionReason.TOO_MANY_REDIRECTS);
        }
    }

    @Override
    public void abort() { }

}
