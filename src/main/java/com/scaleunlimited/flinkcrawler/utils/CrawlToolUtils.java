package com.scaleunlimited.flinkcrawler.utils;

import java.io.IOException;

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.NoopHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.SimpleHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.commoncrawl.CommonCrawlFetcherBuilder;
import com.scaleunlimited.flinkcrawler.tools.CrawlToolOptions;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.NoopUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;

import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.sitemaps.SiteMapParser;

public class CrawlToolUtils {

    // As per https://developers.google.com/search/reference/robots_txt
    private static final int MAX_ROBOTS_TXT_SIZE = 500 * 1024;

    public static BaseUrlLengthener getUrlLengthener(CrawlToolOptions options, UserAgent userAgent) {
        if (options.isNoLengthen()) {
            return new NoopUrlLengthener();
        }

        int maxConnectionsPerHost = options.getFetchersPerTask();
        return new SimpleUrlLengthener(userAgent, maxConnectionsPerHost);
    }

    public static BaseHttpFetcherBuilder getPageFetcherBuilder(CrawlToolOptions options,
            UserAgent userAgent) throws IOException {
        if (options.isCommonCrawl()) {
            return new CommonCrawlFetcherBuilder(options.getFetchersPerTask(), userAgent,
                    options.getCommonCrawlId(), options.getCommonCrawlCacheDir());
        }

        return new SimpleHttpFetcherBuilder(options.getFetchersPerTask(), userAgent)
                .setDefaultMaxContentSize(options.getMaxContentSize());
    }

    public static BaseHttpFetcherBuilder getSitemapFetcherBuilder(CrawlToolOptions options,
            UserAgent userAgent) throws IOException {
        if (options.isCommonCrawl()) {
            // Common crawl index doesn't have sitemap files.
            return new NoopHttpFetcherBuilder(userAgent);
        }

        // By default, give site map fetcher 20% of #threads page fetcher has
        int maxSimultaneousRequests = Math.max(1, options.getFetchersPerTask() / 5);
        return new SimpleHttpFetcherBuilder(maxSimultaneousRequests, userAgent)
                .setDefaultMaxContentSize(SiteMapParser.MAX_BYTES_ALLOWED);
    }

    public static BaseHttpFetcherBuilder getRobotsFetcherBuilder(CrawlToolOptions options,
            UserAgent userAgent) throws IOException {

        // Although the static Common Crawl data does have robots.txt files
        // (in a separate location), there's no index, so it would be ugly to
        // have to download the whole thing. For now, let's just pretend that
        // nobody has a robots.txt file by using a fetcher that always returns
        // a 404.
        if (options.isCommonCrawl()) {
            return new NoopHttpFetcherBuilder(userAgent);
        }

        // By default, give robots fetcher 20% of #threads page fetcher has
        int maxSimultaneousRequests = Math.max(1, options.getFetchersPerTask() / 5);
        return new SimpleHttpFetcherBuilder(maxSimultaneousRequests, userAgent)
                .setDefaultMaxContentSize(MAX_ROBOTS_TXT_SIZE);
    }

}
