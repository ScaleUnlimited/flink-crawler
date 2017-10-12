package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
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

	// TODO pick good time for this
	protected static final long DEFAULT_RETRY_INTERVAL_MS = 100_000 * 1000L;

	private static final int MAX_QUEUED_URLS = 1000;
	
	private static final int MIN_THREAD_COUNT = 10;
	private static final int MAX_THREAD_COUNT = 100;

	private BaseHttpFetcherBuilder _fetcherBuilder;
	private SimpleRobotRulesParser _parser;
	private long _forceCrawlDelay;
	private long _defaultCrawlDelay;
	
	private transient ThreadPoolExecutor _executor;
	private transient BaseHttpFetcher _fetcher;

	// TODO we need a map from domain to sitemap & refresh time, maintained here.
	
	// FUTURE checkpoint the rules.
	// FUTURE expire the rules.
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
		
		BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(MAX_QUEUED_URLS);
		_executor = new ThreadPoolExecutor(MIN_THREAD_COUNT, MAX_THREAD_COUNT, 1L, TimeUnit.SECONDS, workQueue, new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	@Override
	public void close() throws Exception {
		_executor.shutdown();
		
		if (!_executor.awaitTermination(_fetcherBuilder.getTimeoutInSeconds(), TimeUnit.SECONDS)) {
			// TODO handle timeout.
		}
		
		super.close();
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
			// FUTURE use time when robot rules expire to set the refetch time.
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
		int port = url.getPort();
		if (port == -1) {
			return String.format("%s://%s/robots.txt", url.getProtocol(), url.getHostname());
		} else {
			return String.format("%s://%s:%d/robots.txt", url.getProtocol(), url.getHostname(), port);
		}
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
				long robotFetchRetryTime = System.currentTimeMillis();
				BaseRobotRules rules = _rules.get(robotsUrl);
				if (rules != null) {
					collector.collect(processUrl(rules, url));
					return;
				}
				
				try {
					FetchedResult result = _fetcher.get(robotsUrl);
					if (result.getStatusCode() != HttpStatus.SC_OK) {
						rules = _parser.failedFetch(result.getStatusCode());
						// TODO set different retry interval for missing.
						robotFetchRetryTime += 24L * 60 * 60 * 1000;
					} else {
						rules = _parser.parseContent(robotsUrl, result.getContent(), result.getContentType(), _fetcher.getUserAgent().getAgentName());
					}
				} catch (Exception e) {
					rules = _parser.failedFetch(HttpStatus.SC_INTERNAL_SERVER_ERROR);
					// TODO set retry interval for failure.
					robotFetchRetryTime += 24L * 60 * 60 * 1000;
				}
				
				_rules.put(robotsUrl, rules);
				
				List<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> result = new ArrayList<>();
				result.addAll(processUrl(rules, url));
				
				// If we have 
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

}
