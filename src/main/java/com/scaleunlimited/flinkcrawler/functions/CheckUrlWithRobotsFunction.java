package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.http.HttpStatus;

import com.scaleunlimited.flinkcrawler.fetcher.BaseFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.FetchedResult;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;

@SuppressWarnings("serial")
public class CheckUrlWithRobotsFunction extends RichProcessFunction<FetchUrl, Tuple2<CrawlStateUrl, FetchUrl>> {

	// TODO pick good time for this
	private static final long QUEUE_CHECK_DELAY = 10;
	protected static final long DEFAULT_RETRY_INTERVAL_MS = 100_000 * 1000L;

	private static final int MAX_QUEUED_URLS = 1000;
	
	private static final int MIN_THREAD_COUNT = 10;
	private static final int MAX_THREAD_COUNT = 100;

	private BaseFetcher _fetcher;
	private SimpleRobotRulesParser _parser;
	
	private transient ThreadPoolExecutor _executor;

	// TODO we need a map from domain to sitemap & refresh time, maintained here.
	
	// FUTURE checkpoint the rules.
	// FUTURE expire the rules.
	private transient Map<String, BaseRobotRules> _rules;
	private transient ConcurrentLinkedQueue<Tuple2<CrawlStateUrl, FetchUrl>> _output;
	
	public CheckUrlWithRobotsFunction(BaseFetcher fetcher, SimpleRobotRulesParser parser) {
		_fetcher = fetcher;
		_parser = parser;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
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
	public void processElement(final FetchUrl url, Context context, Collector<Tuple2<CrawlStateUrl, FetchUrl>> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);
		
		String robotsKey = null;
		
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
		final FetchUrl robotsUrl = new FetchUrl(robotsKey + "/robots.txt");
		_executor.execute(new Runnable() {
			
			@Override
			public void run() {
				
				long robotFetchRetryTime = System.currentTimeMillis();
				BaseRobotRules rules = null;
				
				try {
					FetchedResult result = _fetcher.get(robotsUrl);
					// TODO if it's a 404, then treat it as allow all
					if (result.getStatusCode() != HttpStatus.SC_OK) {
						rules = _parser.failedFetch(result.getStatusCode());
						// TODO set different retry interval for missing.
						robotFetchRetryTime += 24L * 60 * 60 * 1000;
					} else {
						rules = _parser.parseContent(robotsUrl.getUrl(), result.getContent(), result.getContentType(), _fetcher.getUserAgent().getAgentName());
					}
				} catch (Exception e) {
					rules = _parser.failedFetch(HttpStatus.SC_INTERNAL_SERVER_ERROR);
					// TODO set retry interval for failure.
					robotFetchRetryTime += 24L * 60 * 60 * 1000;
				}
				
				try {
					_rules.put(makeRobotsKey(url), rules);
					_output.add(processUrl(rules, url));
				} catch (MalformedURLException e) {
					_output.add(invalidUrl(url));
				}
			}


		});
		
		// Every time we get called, we'll set up a new timer that fires
		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}

	private Tuple2<CrawlStateUrl, FetchUrl> invalidUrl(FetchUrl url) {
		// TODO log as error, as this should NEVER happen...the URL should be validated by now.
		long now = System.currentTimeMillis();
		CrawlStateUrl crawlStateUrl = new CrawlStateUrl(url.getUrl(), 
														FetchStatus.ERROR_INVALID_URL, 
														url.getPLD(),
														url.getActualScore(), 
														url.getEstimatedScore(), 
														now,
														Long.MAX_VALUE);
		return new Tuple2<CrawlStateUrl, FetchUrl>(crawlStateUrl, null);
	}

	private Tuple2<CrawlStateUrl, FetchUrl> processUrl(BaseRobotRules rules, FetchUrl url) {
		if (rules.isAllowed(url.getUrl())) {
			// Add the crawl delay to the url, so that it can be used to do delay limiting in the fetcher
			url.setCrawlDelay(rules.getCrawlDelay());
			return new Tuple2<CrawlStateUrl, FetchUrl>(null, url);
		} else {
			// FUTURE use time when robot rules expire to set the refetch time.
			long now = System.currentTimeMillis();
			CrawlStateUrl crawlStateUrl = new CrawlStateUrl(url.getUrl(), 
															rules.isDeferVisits() ? FetchStatus.SKIPPED_DEFERRED : FetchStatus.SKIPPED_BLOCKED, 
															url.getPLD(),
															url.getActualScore(), 
															url.getEstimatedScore(), 
															now,
															now + DEFAULT_RETRY_INTERVAL_MS);
			return new Tuple2<CrawlStateUrl, FetchUrl>(crawlStateUrl, null);
		}
	}

	private String makeRobotsKey(FetchUrl url) throws MalformedURLException {
		String actualUrl = url.getUrl();
		URL fullUrl = new URL(actualUrl);
		// TODO should we try to get the default port for the protocol, for cases where getPort returns -1?
		String robotsKey = String.format("%s://%s%s", fullUrl.getProtocol(), fullUrl.getHost(), fullUrl.getPort() == -1 ? "" : ":" + fullUrl.getPort());
		return robotsKey;
	}

	@Override
	public void onTimer(long time, OnTimerContext context, Collector<Tuple2<CrawlStateUrl, FetchUrl>> collector) throws Exception {
		if (!_output.isEmpty()) {
			collector.collect(_output.remove());
		}

		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}

}
