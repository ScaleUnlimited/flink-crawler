package com.scaleunlimited.flinkcrawler.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
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
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.utils.ExceptionUtils;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;


@SuppressWarnings("serial")
public class FetchUrlsFunction extends RichAsyncFunction<FetchUrl, Tuple2<CrawlStateUrl, FetchedUrl>> {
    static final Logger LOGGER = LoggerFactory.getLogger(FetchUrlsFunction.class);
    
	private static final int MIN_THREAD_COUNT = 10;
	private static final int MAX_THREAD_COUNT = 100;
	
	private static final int MAX_QUEUED_URLS = 1000;
	
	private BaseHttpFetcherBuilder _fetcherBuilder;
	private BaseHttpFetcher _fetcher;
	
	private transient ThreadPoolExecutor _executor;
	
	// TODO use native String->long map
	private transient Map<String, Long> _nextFetch;

	/**
	 * Returns a Tuple2 of the CrawlStateUrl and FetchedUrl. In the case of an error while fetching
	 * the FetchedUrl is set to null.
	 * @param fetcherBuider
	 */
	public FetchUrlsFunction(BaseHttpFetcherBuilder fetcherBuilder) {
		_fetcherBuilder = fetcherBuilder;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_fetcher = _fetcherBuilder.build();
		_nextFetch = new ConcurrentHashMap<>();
		
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
	
	@Override
	public void asyncInvoke(FetchUrl url, AsyncCollector<Tuple2<CrawlStateUrl, FetchedUrl>> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);
		
		// TODO immediately skip the URL if the crawl delay hasn't been long enough for this domain. Note that since robots.txt
		// is sub-domain (and port) specific, we have to use the whole domain and port, not just the PLD.
		final String domainKey = String.format("%s://%s:%d", url.getProtocol(), url.getHostname(), url.getPort());
		Long nextFetchTime = _nextFetch.get(domainKey);
		if ((nextFetchTime != null) && (System.currentTimeMillis() < nextFetchTime)) {
			LOGGER.debug("Skipping (crawl-delay) " + url);
			collector.collect(skipUrl(url, nextFetchTime));
			return;
		}
		
		_executor.execute(new Runnable() {
			
			@Override
			public void run() {
				// We need to get a lock on the fetchTime, even though it's concurrent, so that if we add an entry, nobody else is also adding
				// an entry.
				synchronized (_nextFetch) {
					// See if there's been an update since we checked, which might block us.
					Long nextFetchTime = _nextFetch.get(domainKey);
					long currentTime = System.currentTimeMillis();
					if ((nextFetchTime != null) && (currentTime < nextFetchTime)) {
						LOGGER.debug("Skipping (crawl-delay) " + url);
						collector.collect(skipUrl(url, nextFetchTime));
						return;
					}
					
					// We know the next fetch time now.
					// FUTURE - this might not be right, as we could wind up taking a long time to fetch the page.
					_nextFetch.put(domainKey, currentTime + url.getCrawlDelay());
				}
				
				LOGGER.debug("Fetching " + url);
				
				try {
					FetchedResult result = _fetcher.get(url.getUrl(), null);
					FetchedUrl fetchedUrl = new FetchedUrl(url, result.getFetchedUrl(),
														result.getFetchTime(), result.getHeaders(), 
														result.getContent(), result.getContentType(),
														result.getResponseRate());
					
					LOGGER.info("Fetched " + result);
					
					// If we got an error, put null in for fetchedUrl so we don't try to process it downstream.
					if (result.getStatusCode() != HttpStatus.SC_OK) {
						FetchStatus fetchStatus = ExceptionUtils.mapHttpStatusToFetchStatus(result.getStatusCode());
						collector.collect(Collections.singleton(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url, fetchStatus, 0, System.currentTimeMillis(), 0L), null)));
					} else {
						collector.collect(Collections.singleton(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url, FetchStatus.FETCHED, 0, fetchedUrl.getFetchTime(), 0L), fetchedUrl)));
					}
				} catch (Exception e) {
					if (e instanceof BaseFetchException) {
						collector.collect(Collections.singleton(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url, ExceptionUtils.mapExceptionToFetchStatus(e), 0, System.currentTimeMillis(), 0L), null)));
					} else {
						throw new RuntimeException("Exception fetching " + url, e);
					}

				}
			}
		});
	}

	private Collection<Tuple2<CrawlStateUrl, FetchedUrl>> skipUrl(FetchUrl url, Long nextFetchTime) {
		return Collections.singleton(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url, FetchStatus.SKIPPED_CRAWLDELAY, nextFetchTime), null));
	}
	
}
