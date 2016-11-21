package com.scaleunlimited.flinkcrawler.fetcher;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.scaleunlimited.flinkcrawler.config.UserAgent;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public abstract class BaseFetcher implements Serializable {

	protected static final int DEFAULT_MAX_CONTENT_SIZE = 128 * 1024;
    protected static final long DEFAULT_REQUEST_TIMEOUT = 100 * 1000L;
    protected static final RedirectMode DEFAULT_REDIRECT_MODE = RedirectMode.FOLLOW_ALL;
    protected static final int DEFAULT_MAX_REDIRECTS = 20;
    protected static final String DEFAULT_ACCEPT_LANGUAGE = "en-us,en-gb,en;q=0.7,*;q=0.3";
    protected static final int DEFAULT_MIN_RESPONSE_RATE = 100;

	protected UserAgent _userAgent;
	protected Set<String> _validMimeTypes;    // Set of mime-types that we'll accept.
    protected Map<String, Integer> _maxContentSizes;
    protected int _defaultMaxContentSize;
    protected String _acceptLanguage;    		// What to pass for the Accept-Language request header

    protected RedirectMode _redirectMode;
    protected int _maxRedirects;
    protected long _requestTimeout;           // Max time for any given set of URLs (termination timeout is based on this)
    protected int _minResponseRate;        	// lower bounds on bytes-per-second

    protected BaseFetcher(UserAgent userAgent) {
    	_userAgent = userAgent;
    	
    	// Set reasonable defaults for everything else
        _maxContentSizes = new HashMap<String, Integer>();
        _defaultMaxContentSize = DEFAULT_MAX_CONTENT_SIZE;
        _requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        _redirectMode = DEFAULT_REDIRECT_MODE;
        _acceptLanguage = DEFAULT_ACCEPT_LANGUAGE;
        _validMimeTypes = new HashSet<String>();
        _minResponseRate = DEFAULT_MIN_RESPONSE_RATE;
        _maxRedirects = DEFAULT_MAX_REDIRECTS;
    }
    
    public BaseFetcher setUserAgent(UserAgent userAgent) {
        _userAgent = userAgent;
        return this;
    }

    public UserAgent getUserAgent() {
        return _userAgent;
    }
    
    public BaseFetcher setMaxContentSize(String mimeType, int maxContentSize) {
        _maxContentSizes.put(mimeType, maxContentSize);
        return this;
    }

    // TODO - 
    public boolean isValidMimeType(String mimeType) {
    	return _validMimeTypes.isEmpty() || _validMimeTypes.contains(mimeType);
    }
    
    public int getMaxContentSize(String mimeType) {
        Integer result = _maxContentSizes.get(mimeType);
        if (result == null) {
            return _defaultMaxContentSize;
        } else {
        	return result;
        }
    }

    public abstract FetchedResult get(FetchUrl url) throws BaseFetchException;
    
    public abstract void abort();

}
