package com.scaleunlimited.flinkcrawler.urls;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.List;
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
import crawlercommons.fetcher.RedirectFetchException;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

@SuppressWarnings("serial")
public class SimpleUrlLengthener extends BaseUrlLengthener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleUrlLengthener.class);

    private static final Pattern HOSTNAME_PATTERN = Pattern.compile("^https?://([^/:?]{3,})");

    private BaseHttpFetcherBuilder _fetcherBuilder;
    
    private transient BaseHttpFetcher _fetcher;
    private transient Set<String> _urlShorteners;
    
    public SimpleUrlLengthener(UserAgent userAgent) {
        this(FetchUtils.makeRedirectFetcherBuilder(userAgent));
    }

    public SimpleUrlLengthener(BaseHttpFetcherBuilder fetcherBuilder) {
        super();
        _fetcherBuilder = fetcherBuilder;
        try {
            _fetcher = fetcherBuilder.build();
            _urlShorteners = loadUrlShorteners();
        } catch (Exception e) {
            throw new RuntimeException("Unable to build URL lengthener", e);
        }
    }
	
    @Override
    public void open() throws Exception {
        _fetcher = _fetcherBuilder.build();
        _urlShorteners = loadUrlShorteners();
    }

    @Override
	public RawUrl lengthen(RawUrl url) {
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

        try {
            FetchedResult fr = _fetcher.get(urlString);
            int statusCode = fr.getStatusCode();
            if (statusCode == HttpStatus.SC_OK) {
                // This will happen if we're using a fetcher configured to
                // follow redirects (rather than one configured to throw a
                // RedirectFetchException with the details).  This isn't very
                // nice, since we're fetching content from the target site
                // without checking its robot rules, but the caller knows best.
                redirectedUrl = fr.getFetchedUrl();
                LOGGER.debug(String.format("Normal redirection of %s to %s", urlString, redirectedUrl));
            } else {
                LOGGER.trace("Status code " + statusCode + " processing redirect for " + urlString);
            }
        } catch (RedirectFetchException e) {
            // We'll get this exception if our fetcher has been configured with
            // zero redirects to avoid fetching any content from the target URL
            // (as it should be).  (We'll also get this if we're using a fetcher
            // configured to follow redirects, and the target of the link
            // shortening site's redirection is itself redirected).
            // In this case, we've captured the final URL in the exception,
            // so use that for downstream fetching.
            redirectedUrl = e.getRedirectedUrl();
            LOGGER.trace(String.format("Redirecting %s to %s", urlString, redirectedUrl));
        } catch (BaseFetchException e) {
            // The site doesn't seem to like the way we're forcing it to redirect,
            // so just emit the same URL for downstream fetching.
            LOGGER.debug("Exception processing redirect for " + urlString + ": " + e.getMessage(), e);
        }
        
        return new RawUrl(redirectedUrl, url.getScore());
	}

	@Override
	public int getTimeoutInSeconds() {
		return _fetcher.getFetchDurationTimeoutInSeconds();
	}
    
    public static Set<String> loadUrlShorteners() throws IOException {
        Set<String> result = new HashSet<String>();
        List<String> lines = IOUtils.readLines(SimpleUrlLengthener.class.getResourceAsStream("/url-shorteners.txt"), "UTF-8");
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
