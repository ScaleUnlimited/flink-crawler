package com.scaleunlimited.flinkcrawler.parser;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.tika.utils.CharsetUtils;

import com.scaleunlimited.flinkcrawler.config.ParserPolicy;
import com.scaleunlimited.flinkcrawler.metrics.CrawlerAccumulator;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.utils.HttpUtils;

import crawlercommons.util.Headers;

@SuppressWarnings("serial")
public abstract class BasePageParser implements Serializable {

    private ParserPolicy _policy;
	private transient CrawlerAccumulator _crawlerAccumulator;
    
    public BasePageParser(ParserPolicy policy) {
        _policy = policy;
    }

    public ParserPolicy getParserPolicy() {
        return _policy;
    }

    public abstract void open(CrawlerAccumulator crawlerAccumulator) throws Exception;
    public abstract void close() throws Exception;
    
    public abstract ParserResult parse(FetchedUrl fetchedUrl) throws Exception;

    public void setAccumulator(CrawlerAccumulator crawlerAccumulator) {
    	_crawlerAccumulator = crawlerAccumulator;
    }
 
    public CrawlerAccumulator getAccumulator() {
    	return _crawlerAccumulator;
    }

    
    /**
     * Extract encoding from content-type
     * 
     * If a charset is returned, then it's a valid/normalized charset name that's
     * supported on this platform.
     * 
     * @param datum
     * @return charset in response headers, or null
     */
    protected String getCharset(FetchedUrl fetchedUrl) {
        return CharsetUtils.clean(HttpUtils.getCharsetFromContentType(fetchedUrl.getContentType()));
    }

    /**
     * Extract language from (first) explicit header
     * 
     * @param fetchedDatum
     * @param charset 
     * @return first language in response headers, or null
     */
    protected String getLanguage(FetchedUrl fetchedUrl, String charset) {
        return getFirst(fetchedUrl.getHeaders(), HttpUtils.CONTENT_LANGUAGE);
    }

    /**
     * Figure out the right base URL to use, for when we need to resolve relative URLs.
     * 
     * @param fetchedDatum
     * @return the base URL
     * @throws MalformedURLException
     */
    protected URL getContentLocation(FetchedUrl fetchedUrl) throws MalformedURLException {
        URL baseUrl = new URL(fetchedUrl.getFetchedUrl());
        
        // See if we have a content location from the HTTP headers that we should use as
        // the base for resolving relative URLs in the document.
        String clUrl = getFirst(fetchedUrl.getHeaders(), HttpUtils.CONTENT_LOCATION);
        if (clUrl != null) {
            // FUTURE KKr - should we try to keep processing if this step fails, but
            // refuse to resolve relative links?
            baseUrl = new URL(baseUrl, clUrl);
        }
        return baseUrl;
    }

    private String getFirst(Headers headers, String name) {
        String normalizedName = normalize(name);
        List<String> curValues = headers.getValues(normalizedName);
        if ((curValues == null) || curValues.isEmpty()) {
            return null;
        } else {
            return curValues.get(0);
        }
    }

    private static String normalize(String name) {
        return name.toLowerCase();
    }

}
