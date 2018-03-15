package com.scaleunlimited.flinkcrawler.fetcher;

import java.util.HashSet;

import crawlercommons.fetcher.http.UserAgent;

public class FetchUtils {
    
    private static final int URL_LENGTHENER_SITE_TIMEOUT_SECS = 20;

    /**
     * @param userAgent for the fetcher that the builder will construct
     * @return builder for a fetcher configured to avoid fetching any content
     * from the URL but instead to throw an exception with the redirect target
     */
    public static SimpleHttpFetcherBuilder makeRedirectFetcherBuilder(UserAgent userAgent) {
        SimpleHttpFetcherBuilder builder = new SimpleHttpFetcherBuilder(userAgent);
        builder.setDefaultMaxContentSize(0);
        builder.setValidMimeTypes(new HashSet<String>());
        builder.setMaxRedirects(0);
        builder.setFetchDurationTimeoutInSeconds(URL_LENGTHENER_SITE_TIMEOUT_SECS);
        return builder;
    }
}
