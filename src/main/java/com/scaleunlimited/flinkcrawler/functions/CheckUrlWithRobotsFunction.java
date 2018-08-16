package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;
import com.scaleunlimited.flinkcrawler.tools.CrawlTool;

import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;

/**
 * We get passed a URL to fetch, that we check against the domain's robots.txt rules. The resulting
 * Tuple3 will have only one of the three fields set to a none-null value, based on the result (and
 * optional extraction of sitemap URLs):
 * 
 * Blocked: first field (CrawlStateUrl) is set to a blocked by robots status Allowed: second field
 * (FetchUrl) is set to be the same as the incoming FetchUrl Sitemap: third field (FetchUrl) is set
 * to the sitemap URL. There can be multiple sitemaps, in which case the Collection we pass to the
 * collector will have multiple Tuple3<> values. Sitemap URLs that are invalid are logged and
 * dropped.
 *
 */
@SuppressWarnings("serial")
public class CheckUrlWithRobotsFunction
        extends BaseAsyncFunction<FetchUrl, Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> {
    static final Logger LOGGER = LoggerFactory.getLogger(CheckUrlWithRobotsFunction.class);

    // FUTURE pick good time for this.
    // See https://github.com/ScaleUnlimited/flink-crawler/issues/53
    protected static final long DEFAULT_RETRY_INTERVAL_MS = 100_000 * 1000L;

    // Make this controllable from the command line
    private static final int THREAD_COUNT = 10;

    private BaseHttpFetcherBuilder _fetcherBuilder;
    private SimpleRobotRulesParser _parser;
    private long _forceCrawlDelay;
    private long _defaultCrawlDelay;

    private long _numMissingRobots;
    private long _numFetchedRobots;
    private long _numFailedRobots;

    private transient BaseHttpFetcher _fetcher;

    // FUTURE checkpoint the rules.
    // See https://github.com/ScaleUnlimited/flink-crawler/issues/55

    private transient Map<String, BaseRobotRules> _rules;
    private transient Map<String, Long> _ruleExpirations;

    public CheckUrlWithRobotsFunction(BaseHttpFetcherBuilder fetcherBuilder,
            SimpleRobotRulesParser parser, long forceCrawlDelay, long defaultCrawlDelay) {
        super(THREAD_COUNT, fetcherBuilder.getFetchDurationTimeoutInSeconds());

        _fetcherBuilder = fetcherBuilder;
        _parser = parser;
        _forceCrawlDelay = forceCrawlDelay;
        _defaultCrawlDelay = defaultCrawlDelay;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        _fetcher = _fetcherBuilder.build();
        _rules = new ConcurrentHashMap<>();
        _ruleExpirations = new ConcurrentHashMap<>();
    }

    @Override
    public void asyncInvoke(FetchUrl url,
            ResultFuture<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> future) throws Exception {
        record(this.getClass(), url);

        LOGGER.trace("Queueing '{}' for robots check", url);
        _executor.execute(new Runnable() {

            @Override
            public void run() {
                final String robotsUrl = makeRobotsKey(url);
                // TODO make _rules a class that manages both the rules and the expiration
                // time, so we don't have to worry about edge cases with expiration and
                // rules maps potentially getting out of sync in this multi-threaded environment.
                BaseRobotRules rules = _rules.get(robotsUrl);
                if (rules != null) {
                    // See if the rule should be expired.
                    if (System.currentTimeMillis() >= _ruleExpirations.get(robotsUrl)) {
                        _rules.remove(robotsUrl);
                        _ruleExpirations.remove(robotsUrl);
                    } else {
                        LOGGER.trace("Found cached rule for '{}', collecting", url);
                        future.complete(processUrl(rules, url));
                        return;
                    }
                }

                int httpStatusCode;
                long robotsFetchRetryDelay;
                try {
                    FetchedResult result = _fetcher.get(robotsUrl);
                    httpStatusCode = result.getStatusCode();
                    LOGGER.trace("CheckUrlWithRobotsFunction fetched URL '{}' with status {}",
                            robotsUrl, httpStatusCode);
                    robotsFetchRetryDelay = calcRobotsFetchRetryDelay(httpStatusCode);
                    if (httpStatusCode != HttpStatus.SC_OK) {
                        rules = _parser.failedFetch(httpStatusCode);
                        if ((httpStatusCode >= 400) && (httpStatusCode < 500)) {
                            _numMissingRobots++;
                        } else {
                            _numFailedRobots++;
                        }
                    } else {
                        rules = _parser.parseContent(robotsUrl, result.getContent(),
                                result.getContentType(), _fetcher.getUserAgent().getAgentName());
                        _numFetchedRobots++;
                    }
                } catch (Exception e) {
                    LOGGER.error(String.format("Unexpected error while fetching robots file for '%s'", url.getHostname()), e);
                    httpStatusCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
                    robotsFetchRetryDelay = calcRobotsFetchRetryDelay(httpStatusCode);
                    rules = _parser.failedFetch(httpStatusCode);
                    _numFailedRobots++;
                }

                // Set re-fetch time for robots. Note that we put the expiration first, so that
                // by the time someone checks _rules, the expiration entry exists.
                _ruleExpirations.put(robotsUrl, System.currentTimeMillis() + robotsFetchRetryDelay);
                BaseRobotRules oldRules = _rules.put(robotsUrl, rules);
                if (oldRules == null) {
                    // Output counters whenever we encounter a new domain
                    printCounters(url.getHostname(), httpStatusCode);
                }

                List<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> result = new ArrayList<>();
                result.addAll(processUrl(rules, url));

                // If we have sitemaps, process them now.
                List<String> sitemaps = rules.getSitemaps();
                if ((sitemaps != null) && !sitemaps.isEmpty()) {

                    // Output the sitemap urls in the tuple3
                    for (String sitemap : sitemaps) {
                        try {
                            result.add(new Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>(null, null,
                                    new FetchUrl(new ValidUrl(sitemap))));
                        } catch (MalformedURLException e) {
                            LOGGER.warn("Invalid sitemap URL '{}' from '{}'", robotsUrl, sitemap);
                        }
                    }
                }

                LOGGER.trace("Completing robots results for '{}'", url);
                future.complete(result);
            }

        });
    }

    /**
     * Given the result of trying to fetch the robots.txt file, decide how long until we retry (or
     * refetch) it again.
     * 
     * @param statusCode
     * @return interval to wait, in milliseconds.
     */
    private long calcRobotsFetchRetryDelay(int statusCode) {
        if (statusCode == HttpStatus.SC_OK) {
            return 12L * 60 * 60 * 1000;
        } else if (statusCode == HttpStatus.SC_NOT_FOUND) {
            return 24L * 60 * 60 * 1000;
        } else if (statusCode == HttpStatus.SC_INTERNAL_SERVER_ERROR) {
            return 1L * 60 * 60 * 1000;
        } else {
            // Other errors usually indicate that the server is miss-configured,
            // and we really want to treat it as a "not found" (even though we
            // don't currently).
            return 24L * 60 * 60 * 1000;
        }
    }

    private Collection<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> processUrl(BaseRobotRules rules,
            FetchUrl url) {
        if (rules.isAllowed(url.getUrl())) {
            // Add the crawl delay to the url, so that it can be used to do delay limiting in the
            // fetcher
            long crawlDelay = _forceCrawlDelay;
            if (_forceCrawlDelay == CrawlTool.DO_NOT_FORCE_CRAWL_DELAY) {
                crawlDelay = rules.getCrawlDelay();
                if (crawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) {
                    crawlDelay = _defaultCrawlDelay;
                }
            }
            url.setCrawlDelay(crawlDelay);
            return Collections
                    .singleton(new Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>(null, url, null));
        } else {
            // FUTURE use time when robot rules expire to set the refetch time. If it's a
            // skipped-deferred (couldn't fetch robots.txt due to server error) then that robots
            // refetch time should be much shorter. Note that we want to make this refetch time
            // a bit longer than the robots.txt reload/retry time, so that it can be reloaded.
            // See https://github.com/ScaleUnlimited/flink-crawler/issues/53
            long now = System.currentTimeMillis();
            CrawlStateUrl crawlStateUrl = new CrawlStateUrl(url,
                    rules.isDeferVisits() ? FetchStatus.SKIPPED_DEFERRED
                            : FetchStatus.SKIPPED_BLOCKED,
                    now);
            crawlStateUrl.setScore(url.getScore());
            crawlStateUrl.setNextFetchTime(now + DEFAULT_RETRY_INTERVAL_MS);
            return Collections.singleton(
                    new Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>(crawlStateUrl, null, null));
        }
    }


    private String makeRobotsKey(FetchUrl url) {
        return String.format("%s/robots.txt", url.getUrlWithoutPath());
    }

    private void printCounters(String hostname, int status) {
        if (LOGGER.isDebugEnabled()) {
            String counters = String.format(
                    "Domain: %s, status: %d, missing: %d, good: %d, bad: %d", hostname,
                    status, _numMissingRobots, _numFetchedRobots, _numFailedRobots);
            LOGGER.debug(counters);
        }
    }
}
