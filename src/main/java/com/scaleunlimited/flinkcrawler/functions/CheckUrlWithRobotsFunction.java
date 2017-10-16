package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;
import com.scaleunlimited.flinkcrawler.tools.CrawlTool;
import com.scaleunlimited.flinkcrawler.utils.ThreadedExecutor;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;

/**
 * We get passed a URL to fetch, that we check against the domain's robots.txt rules. The resulting Tuple3 will
 * have only one of the three fields set to a none-null value, based on the result (and optional extraction of
 * sitemap URLs):
 * 
 * Blocked: first field (CrawlStateUrl) is set to a blocked by robots status
 * Allowed: second field (FetchUrl) is set to be the same as the incoming FetchUrl
 * Sitemap: third field (FetchUrl) is set to the sitemap URL. There can be multiple sitemaps, in which case the
 *          Collection we pass to the collector will have multiple Tuple3<> values. Sitemap URLs that are invalid
 *          are logged and dropped.
 *
 */
@SuppressWarnings("serial")
public class CheckUrlWithRobotsFunction extends RichAsyncFunction<FetchUrl, Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> {
    static final Logger LOGGER = LoggerFactory.getLogger(CheckUrlWithRobotsFunction.class);

	// FUTURE pick good time for this.
    // See https://github.com/ScaleUnlimited/flink-crawler/issues/53
	protected static final long DEFAULT_RETRY_INTERVAL_MS = 100_000 * 1000L;

	// Make this controllable from the command line
	private static final int THREAD_COUNT = 100;

	private BaseHttpFetcherBuilder _fetcherBuilder;
	private SimpleRobotRulesParser _parser;
	private long _forceCrawlDelay;
	private long _defaultCrawlDelay;
	
	private transient ThreadedExecutor _executor;
	private transient BaseHttpFetcher _fetcher;

	// TODO we need a map from domain to sitemap & refresh time, maintained here.
	
	// FUTURE checkpoint the rules.
	// See https://github.com/ScaleUnlimited/flink-crawler/issues/55
	
	// FUTURE expire the rules.
	// See https://github.com/ScaleUnlimited/flink-crawler/issues/54
	private transient Map<String, BaseRobotRules> _rules;
	
	public CheckUrlWithRobotsFunction(BaseHttpFetcherBuilder fetcherBuider, SimpleRobotRulesParser parser, long forceCrawlDelay, long defaultCrawlDelay) {
		_fetcherBuilder = fetcherBuider;
		_parser = parser;
		_forceCrawlDelay = forceCrawlDelay;
		_defaultCrawlDelay = defaultCrawlDelay;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_fetcher = _fetcherBuilder.build();
		_rules = new ConcurrentHashMap<>();
		_executor = new ThreadedExecutor(THREAD_COUNT);
	}
	
	@Override
	public void close() throws Exception {
		_executor.terminate(_fetcherBuilder.getTimeoutInSeconds(), TimeUnit.SECONDS);
		
		super.close();
	}
	
	@Override
	public void asyncInvoke(FetchUrl url, AsyncCollector<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);

		final String robotsUrl = makeRobotsKey(url);
		BaseRobotRules rules = _rules.get(robotsUrl);
		if (rules != null) {
			collector.collect(processUrl(rules, url));
			return;
		}

		// We don't have robots yet, so queue up the URL for fetching code
		_executor.execute(new Runnable() {

			@Override
			public void run() {
				// We might, in the thread, get a URL that we've already processed because multiple were queued up.
				// So do the check again, then fetch if needed. We might also have multiple threads processing URLs
				// for the same domain, but for now let's not worry about that case (just slightly less efficient).
				BaseRobotRules rules = _rules.get(robotsUrl);
				if (rules != null) {
					collector.collect(processUrl(rules, url));
					return;
				}

				long robotFetchRetryTime = System.currentTimeMillis();
				try {
					FetchedResult result = _fetcher.get(robotsUrl);
					LOGGER.debug(String.format("CheckUrlWithRobotsFunction fetched URL %s with status %d", robotsUrl, result.getStatusCode()));
					if (result.getStatusCode() != HttpStatus.SC_OK) {
						rules = _parser.failedFetch(result.getStatusCode());
						// FUTURE set different retry interval for missing.
						// See https://github.com/ScaleUnlimited/flink-crawler/issues/54
						robotFetchRetryTime += 24L * 60 * 60 * 1000;
					} else {
						rules = _parser.parseContent(robotsUrl, result.getContent(), result.getContentType(), _fetcher.getUserAgent().getAgentName());
					}
				} catch (Exception e) {
					rules = _parser.failedFetch(HttpStatus.SC_INTERNAL_SERVER_ERROR);
					// FUTURE set different rery interval for failure.
					// See https://github.com/ScaleUnlimited/flink-crawler/issues/54
					robotFetchRetryTime += 24L * 60 * 60 * 1000;
				}

				// FUTURE set re-fetch time for robots.
				// See https://github.com/ScaleUnlimited/flink-crawler/issues/54
				_rules.put(robotsUrl, rules);

				List<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> result = new ArrayList<>();
				result.addAll(processUrl(rules, url));

				// If we have sitemaps, process them now.
				List<String> sitemaps = rules.getSitemaps();
				if ((sitemaps != null) && !sitemaps.isEmpty()) {

					// Output the sitemap urls in the tuple3
					for (String sitemap : sitemaps) {
						try {
							result.add(new Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>(null, null, new FetchUrl(new ValidUrl(sitemap))));
						} catch (MalformedURLException e) {
							LOGGER.info(String.format("Invalid sitemap URL from %s: %s", robotsUrl, sitemap));
						}
					}
				}

				collector.collect(result);
			}
		});
	}

	private Collection<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> processUrl(BaseRobotRules rules, FetchUrl url) {
		if (rules.isAllowed(url.getUrl())) {
			// Add the crawl delay to the url, so that it can be used to do delay limiting in the fetcher
			long crawlDelay = _forceCrawlDelay;
			if (_forceCrawlDelay == CrawlTool.DO_NOT_FORCE_CRAWL_DELAY) {
				crawlDelay = rules.getCrawlDelay();
				if (crawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) {
					crawlDelay = _defaultCrawlDelay;
				}
			}
			
			url.setCrawlDelay(crawlDelay);
			return Collections.singleton(new Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>(null, url, null));
		} else {
			// FUTURE use time when robot rules expire to set the refetch time. If it's a
			// skipped-deferred (couldn't fetch robots.txt due to server error) then that robots
			// refetch time should be much shorter. Note that we want to make this refetch time
			// a bit longer than the robots.txt reload/retry time, so that it can be reloaded.
			// See https://github.com/ScaleUnlimited/flink-crawler/issues/53
			long now = System.currentTimeMillis();
			CrawlStateUrl crawlStateUrl = new CrawlStateUrl(url,
															rules.isDeferVisits() ? FetchStatus.SKIPPED_DEFERRED : FetchStatus.SKIPPED_BLOCKED, 
															now,
															url.getScore(), 
															now + DEFAULT_RETRY_INTERVAL_MS);
			return Collections.singleton(new Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>(crawlStateUrl, null, null));
		}
	}

	private String makeRobotsKey(FetchUrl url) {
		return String.format("%s/robots.txt", url.getUrlWithoutPath());
	}


}
