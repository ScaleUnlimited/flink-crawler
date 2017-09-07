package com.scaleunlimited.flinkcrawler.parser;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.tika.utils.CharsetUtils;

import com.scaleunlimited.flinkcrawler.config.ParserPolicy;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.utils.HttpUtils;

import crawlercommons.util.Headers;

@SuppressWarnings("serial")
public abstract class BasePageParser implements Serializable {

	// TODO VMa - move these into a common http headers file ?
    public final static String CONTENT_LANGUAGE = "Content-Language";
    public final static String CONTENT_LOCATION = "Content-Location";

    private ParserPolicy _policy;
    
    public BasePageParser(ParserPolicy policy) {
        _policy = policy;
    }

    public ParserPolicy getParserPolicy() {
        return _policy;
    }

    public abstract ParserResult parse(FetchedUrl fetchedUrl) throws Exception;

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
        return getFirst(fetchedUrl.getHeaders(), CONTENT_LANGUAGE);
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
        String clUrl = getFirst(fetchedUrl.getHeaders(), CONTENT_LOCATION);
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
