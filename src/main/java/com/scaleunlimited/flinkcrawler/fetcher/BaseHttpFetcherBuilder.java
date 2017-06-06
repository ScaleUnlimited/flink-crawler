package com.scaleunlimited.flinkcrawler.fetcher;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import crawlercommons.fetcher.BaseFetcher;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.BaseHttpFetcher.RedirectMode;
import crawlercommons.fetcher.http.UserAgent;

@SuppressWarnings("serial")
public abstract class BaseHttpFetcherBuilder implements Serializable {
	
	// From BaseFetcher:
    protected Map<String, Integer> _maxContentSizes = new HashMap<String, Integer>();
    protected int _defaultMaxContentSize = BaseFetcher.DEFAULT_MAX_CONTENT_SIZE;
    protected Set<String> _validMimeTypes = new HashSet<String>();

    // From BaseHttpFetcher:
    protected int _maxThreads;
    protected UserAgent _userAgent;
    protected int _maxRedirects = BaseHttpFetcher.DEFAULT_MAX_REDIRECTS;
    protected int _maxConnectionsPerHost = BaseHttpFetcher.DEFAULT_MAX_CONNECTIONS_PER_HOST;
    protected int _minResponseRate = BaseHttpFetcher.DEFAULT_MIN_RESPONSE_RATE;
    protected String _acceptLanguage = BaseHttpFetcher.DEFAULT_ACCEPT_LANGUAGE;
    protected RedirectMode _redirectMode = BaseHttpFetcher.DEFAULT_REDIRECT_MODE;

    public BaseHttpFetcherBuilder(int maxThreads, UserAgent userAgent) {
    	super();
        
        _maxThreads = maxThreads;
        _userAgent = userAgent;
    }
    
	// From BaseFetcher:
    
    public BaseHttpFetcherBuilder setDefaultMaxContentSize(int defaultMaxContentSize) {
        _defaultMaxContentSize = defaultMaxContentSize;
        return this;
    }
    
    public BaseHttpFetcherBuilder setMaxContentSize(String mimeType, int maxContentSize) {
        _maxContentSizes.put(mimeType, maxContentSize);
        return this;
    }

    public BaseHttpFetcherBuilder setValidMimeTypes(Set<String> validMimeTypes) {
        _validMimeTypes = new HashSet<String>(validMimeTypes);
        return this;
    }

    // From BaseHttpFetcher:
    
    public BaseHttpFetcherBuilder setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        _maxConnectionsPerHost = maxConnectionsPerHost;
        return this;
    }
    
    public BaseHttpFetcherBuilder setMinResponseRate(int minResponseRate) {
        _minResponseRate = minResponseRate;
        return this;
    }

    public BaseHttpFetcherBuilder setAcceptLanguage(String acceptLanguage) {
        _acceptLanguage = acceptLanguage;
        return this;
    }
    
    public BaseHttpFetcherBuilder setMaxRedirects(int maxRedirects) {
        _maxRedirects = maxRedirects;
        return this;
    }
    
    public BaseHttpFetcherBuilder setRedirectMode(RedirectMode mode) {
        _redirectMode = mode;
        return this;
    }
    
    /**
     * @return a new BaseHttpFetcher instance configured to match how this
     * builder was configured
     */
    public abstract BaseHttpFetcher build();
    
    /**
     * Helper method that {@link #build()} can use to configure a newly
     * constructed BaseHttpFetcher instance
     * 
     * @param fetcher instance of BaseHttpFetcher that {@link #build()} has
     * just constructed
     * @return the same fetcher, after applying all of the configuration
     * settings from this builder to that BaseHttpFetcher instance
     */
    protected BaseHttpFetcher configure(BaseHttpFetcher fetcher) {
    	fetcher.setDefaultMaxContentSize(_defaultMaxContentSize);
		for (Map.Entry<String, Integer> entry : _maxContentSizes.entrySet()) {
			fetcher.setMaxContentSize(entry.getKey(), entry.getValue());
		}
		fetcher.setValidMimeTypes(_validMimeTypes);
		fetcher.setMaxConnectionsPerHost(_maxConnectionsPerHost);
		fetcher.setMinResponseRate(_minResponseRate);
		fetcher.setAcceptLanguage(_acceptLanguage);
		fetcher.setMaxRedirects(_maxRedirects);
		fetcher.setRedirectMode(_redirectMode);
    	return fetcher;
    }
}
