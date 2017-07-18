package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.http.HttpStatus;

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;

@SuppressWarnings("serial")
public class CheckUrlWithRobotsFunction extends RichProcessFunction<FetchUrl, Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> {

	// TODO pick good time for this
	private static final long QUEUE_CHECK_DELAY = 10;
	protected static final long DEFAULT_RETRY_INTERVAL_MS = 100_000 * 1000L;

	private static final int MAX_QUEUED_URLS = 1000;
	
	private static final int MIN_THREAD_COUNT = 10;
	private static final int MAX_THREAD_COUNT = 100;

	private BaseHttpFetcherBuilder _fetcherBuilder;
	private BaseHttpFetcher _fetcher;
	private SimpleRobotRulesParser _parser;
	private long _defaultCrawlDelay;
	
	private transient ThreadPoolExecutor _executor;

	// TODO we need a map from domain to sitemap & refresh time, maintained here.
	
	// FUTURE checkpoint the rules.
	// FUTURE expire the rules.
	private transient Map<String, BaseRobotRules> _rules;
	private transient ConcurrentLinkedQueue<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> _output;
	
	public CheckUrlWithRobotsFunction(BaseHttpFetcherBuilder fetcherBuider, SimpleRobotRulesParser parser, long defaultCrawlDelay) {
		_fetcherBuilder = fetcherBuider;
		_parser = parser;
		_defaultCrawlDelay = defaultCrawlDelay;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_fetcher = _fetcherBuilder.build();
		_output = new ConcurrentLinkedQueue<>();
		_rules = new ConcurrentHashMap<>();
		
		BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(MAX_QUEUED_URLS);
		_executor = new ThreadPoolExecutor(MIN_THREAD_COUNT, MAX_THREAD_COUNT, 1L, TimeUnit.SECONDS, workQueue, new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	@Override
	public void close() throws Exception {
		_executor.shutdown();
		
		// TODO get timeout from fetcher
		if (!_executor.awaitTermination(1, TimeUnit.SECONDS)) {
			// TODO handle timeout.
		}
		
		super.close();
	}
	
	@Override
	public void processElement(final FetchUrl url, Context context, Collector<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);
		
		final String robotsKey;
		
		try {
			robotsKey = makeRobotsKey(url);
			BaseRobotRules rules = _rules.get(robotsKey);
			if (rules != null) {
				collector.collect(processUrl(rules, url));
				return;
			}
		} catch (MalformedURLException e) {
			collector.collect(invalidUrl(url));
			return;
		}
		
		// We don't have robots yet, so queue up the URL for fetching code
		final String robotsUrl = robotsKey + "/robots.txt";
		_executor.execute(new Runnable() {
			
			@Override
			public void run() {
				// We might, in the thread, get a URL that we've already processed because multiple were queued up.
				// So do the check again, then fetch if needed. We might also have multiple threads processing URLs
				// for the same domain, but for now let's not worry about that case (just slightly less efficient).
				long robotFetchRetryTime = System.currentTimeMillis();
				BaseRobotRules rules = _rules.get(robotsKey);
				if (rules != null) {
					_output.add(processUrl(rules, url));
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
				
				List<String> sitemaps = rules.getSitemaps();
				try {
					if (sitemaps != null && sitemaps.size() > 0) {
						// Output the sitemap urls in the tuple3
						for (String sitemap : sitemaps) {
							_output.add(new Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>(null, null, new FetchUrl(new ValidUrl(sitemap))));
						}
					} else {
						// TODO use robotFetchRetryTime to set up when we want to purge our rules
							_output.add(processUrl(rules, url));
					}
					_rules.put(makeRobotsKey(url), rules);
				} catch (MalformedURLException e) {
					_output.add(invalidUrl(url));
				}
			}
		});
		
		// Every time we get called, we'll set up a new timer that fires
		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}

	private Tuple3<CrawlStateUrl, FetchUrl, FetchUrl> invalidUrl(FetchUrl url) {
		long now = System.currentTimeMillis();
		CrawlStateUrl crawlStateUrl = new CrawlStateUrl(url, 
														FetchStatus.ERROR_INVALID_URL, 
														url.getScore(), 
														now,
														Long.MAX_VALUE);
		return new Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>(crawlStateUrl, null, null);
	}

	private Tuple3<CrawlStateUrl, FetchUrl, FetchUrl> processUrl(BaseRobotRules rules, FetchUrl url) {
		if (rules.isAllowed(url.getUrl())) {
			// Add the crawl delay to the url, so that it can be used to do delay limiting in the fetcher
			long crawlDelay = rules.getCrawlDelay();
			if (crawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) {
				crawlDelay = _defaultCrawlDelay;
			}
			
			url.setCrawlDelay(crawlDelay);
			return new Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>(null, url, null);
		} else {
			// FUTURE use time when robot rules expire to set the refetch time.
			long now = System.currentTimeMillis();
			CrawlStateUrl crawlStateUrl = new CrawlStateUrl(url,
															rules.isDeferVisits() ? FetchStatus.SKIPPED_DEFERRED : FetchStatus.SKIPPED_BLOCKED, 
															url.getScore(), 
															now,
															now + DEFAULT_RETRY_INTERVAL_MS);
			return new Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>(crawlStateUrl, null, null);
		}
	}

	private String makeRobotsKey(FetchUrl url) throws MalformedURLException {
		int port = url.getPort();
		if (port == -1) {
			return String.format("%s://%s", url.getProtocol(), url.getHostname());
		} else {
			return String.format("%s://%s:%d", url.getProtocol(), url.getHostname(), port);
		}
	}

	@Override
	public void onTimer(long time, OnTimerContext context, Collector<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> collector) throws Exception {
		if (!_output.isEmpty()) {
			collector.collect(_output.remove());
		}

		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}

}
