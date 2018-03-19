package com.scaleunlimited.flinkcrawler.urls;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.FetchUtils;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.util.Headers;

@SuppressWarnings("serial")
public class SimpleUrlLengthener extends BaseUrlLengthener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleUrlLengthener.class);

    private static final Pattern HOSTNAME_PATTERN = Pattern.compile("^https?://([^/:?]{3,})");

    private static final int LRU_CACHE_CAPACITY = 10_000;

    private static final float LRU_CACHE_LOAD_FACTOR = 0.75f;

    private BaseHttpFetcherBuilder _fetcherBuilder;

    private transient BaseHttpFetcher _fetcher;
    private transient Set<String> _urlShorteners;
    private transient Map<RawUrl, RawUrl> _lengthenedUrlMap;

    public SimpleUrlLengthener(UserAgent userAgent, int maxConnectionsPerHost) {
        this(FetchUtils.makeRedirectFetcherBuilder(maxConnectionsPerHost, userAgent)
                .setMaxConnectionsPerHost(maxConnectionsPerHost));
    }

    public SimpleUrlLengthener(BaseHttpFetcherBuilder fetcherBuilder) {
        super();
        _fetcherBuilder = fetcherBuilder;
    }

    @Override
    public void open() throws Exception {
        _fetcher = _fetcherBuilder.build();
        _urlShorteners = loadUrlShorteners();
        _lengthenedUrlMap = new LinkedHashMap<RawUrl, RawUrl>(  (LRU_CACHE_CAPACITY * 4/3),
                                                                LRU_CACHE_LOAD_FACTOR, 
                                                                true); // LRU
    }

    @Override
    public RawUrl lengthen(RawUrl url) {
        
        if (_lengthenedUrlMap.containsKey(url)) {
            return _lengthenedUrlMap.get(url);
        }

        // If the domain is a link shortener, lengthen it.

        String urlString = url.getUrl();
        
        Matcher m = HOSTNAME_PATTERN.matcher(urlString);
        if (!m.find()) {
            return url;
        }

        String hostname = m.group(1);
        if (!_urlShorteners.contains(hostname)) {
            // FUTURE - see if this looks like a shortened URL
            return url;
        }

        String redirectedUrl = urlString;
        LOGGER.debug(String.format("Checking redirection of '%s'", urlString));

        try {
            FetchedResult fr = _fetcher.get(urlString);
            int statusCode = fr.getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                // This will happen if we're using a fetcher configured to
                // follow redirects (rather than one configured to immediately
                // return a 301). This isn't very nice, since we're fetching
                // content from the target site without checking its robot rules,
                // but the caller knows best.
                redirectedUrl = fr.getFetchedUrl();
                LOGGER.debug(
                        String.format("Normal redirection of %s to %s", urlString, redirectedUrl));
            } else if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY) {
                redirectedUrl = extractRedirectUrl(fr, urlString);
                LOGGER.debug(String.format("Redirecting %s to %s", urlString, redirectedUrl));
            } else {
                LOGGER.debug(String.format("Status code %d processing redirect for '%s'",
                        statusCode, urlString));
            }
        } catch (BaseFetchException e) {
            // The site doesn't seem to like the way we're forcing it to redirect,
            // so just emit the same URL for downstream fetching.
            LOGGER.debug("Exception processing redirect for " + urlString + ": " + e.getMessage(),
                    e);
        }

        RawUrl result = new RawUrl(redirectedUrl, url.getScore());
        _lengthenedUrlMap.put(url, result);
        return result;
    }

    private String extractRedirectUrl(FetchedResult fr, String originalUrlAsString) {
        String redirectUrlAsString = fr.getHeaders().get(Headers.LOCATION);
        if (redirectUrlAsString == null) {
            LOGGER.warn("No redirect location available for: " + originalUrlAsString);
            return originalUrlAsString;
        }

        try {
            new URL(redirectUrlAsString);
            return redirectUrlAsString;
        } catch (MalformedURLException e) {
            LOGGER.warn("Malformed location for redirect: " + redirectUrlAsString);
            return originalUrlAsString;
        }
    }

    @Override
    public int getTimeoutInSeconds() {
        if (_fetcher == null) {
            return _fetcherBuilder.getFetchDurationTimeoutInSeconds();
        }
        return _fetcher.getFetchDurationTimeoutInSeconds();
    }

    public static Set<String> loadUrlShorteners() throws IOException {
        Set<String> result = new HashSet<String>();
        List<String> lines = IOUtils.readLines(
                SimpleUrlLengthener.class.getResourceAsStream("/url-shorteners.txt"), "UTF-8");
        for (String line : lines) {
            line = line.trim();
            if ((line.length() == 0) || (line.startsWith("#"))) {
                continue;
            }

            int commentIndex = line.indexOf('#');
            if (commentIndex != -1) {
                line = line.substring(0, commentIndex).trim();
            }

            result.add(line);
        }

        return result;
    }
}
