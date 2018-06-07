package com.scaleunlimited.flinkcrawler.parser;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.tika.utils.CharsetUtils;

import com.scaleunlimited.flinkcrawler.config.ParserPolicy;
import com.scaleunlimited.flinkcrawler.focused.BasePageScorer;
import com.scaleunlimited.flinkcrawler.metrics.CrawlerAccumulator;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;
import com.scaleunlimited.flinkcrawler.utils.HttpUtils;

import crawlercommons.util.Headers;

@SuppressWarnings("serial")
public abstract class BasePageParser implements Serializable {

    private ParserPolicy _policy;
    private BasePageScorer _pageScorer;
    private transient CrawlerAccumulator _accumulator;

    public BasePageParser(ParserPolicy policy, BasePageScorer pageScorer) {
        _policy = policy;
        _pageScorer = pageScorer;
    }

    public ParserPolicy getParserPolicy() {
        return _policy;
    }

    public BasePageScorer getPageScorer() {
        return _pageScorer;
    }

    public void open(RuntimeContext context) throws Exception {
        _accumulator = new CrawlerAccumulator(context);
        _pageScorer.open(context);
    }

    public void close() throws Exception {
        _pageScorer.close();
    }

    public abstract ParserResult parse(FetchResultUrl fetchedUrl) throws Exception;

    protected CrawlerAccumulator getAccumulator() {
        return _accumulator;
    }

    /**
     * Extract encoding from content-type
     * 
     * If a charset is returned, then it's a valid/normalized charset name that's supported on this
     * platform.
     * 
     * @param datum
     * @return charset in response headers, or null
     */
    protected String getCharset(FetchResultUrl fetchedUrl) {
        return CharsetUtils.clean(HttpUtils.getCharsetFromContentType(fetchedUrl.getContentType()));
    }

    /**
     * Extract language from (first) explicit header
     * 
     * @param fetchedDatum
     * @param charset
     * @return first language in response headers, or null
     */
    protected String getLanguage(FetchResultUrl fetchedUrl, String charset) {
        return getFirst(fetchedUrl.getHeaders(), HttpUtils.CONTENT_LANGUAGE);
    }

    /**
     * Figure out the right base URL to use, for when we need to resolve relative URLs.
     * 
     * @param fetchedDatum
     * @return the base URL
     * @throws MalformedURLException
     */
    protected URL getContentLocation(FetchResultUrl fetchedUrl) throws MalformedURLException {
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
