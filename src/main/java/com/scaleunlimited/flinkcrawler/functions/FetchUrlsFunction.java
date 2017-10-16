package com.scaleunlimited.flinkcrawler.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
import com.scaleunlimited.flinkcrawler.utils.ThreadedExecutor;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;


@SuppressWarnings("serial")
public class FetchUrlsFunction extends RichAsyncFunction<FetchUrl, Tuple2<CrawlStateUrl, FetchedUrl>> {
    static final Logger LOGGER = LoggerFactory.getLogger(FetchUrlsFunction.class);
    
	public static final int DEFAULT_THREAD_COUNT = 100;
	
	private int _threadCount;
	private BaseHttpFetcherBuilder _fetcherBuilder;
	private BaseHttpFetcher _fetcher;
	
	private transient ThreadedExecutor _executor;
	
	// TODO use native String->long map
	private transient Map<String, Long> _nextFetch;

	/**
	 * Returns a Tuple2 of the CrawlStateUrl and FetchedUrl. In the case of an error while fetching
	 * the FetchedUrl is set to null.
	 * @param fetcherBuider
	 */
	public FetchUrlsFunction(BaseHttpFetcherBuilder fetcherBuilder) {
		this(fetcherBuilder, DEFAULT_THREAD_COUNT);
	}

	public FetchUrlsFunction(	BaseHttpFetcherBuilder fetcherBuilder,
								int threadCount) {
		_fetcherBuilder = fetcherBuilder;
		_threadCount = threadCount;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_fetcher = _fetcherBuilder.build();
		_nextFetch = new HashMap<>();
		_executor = new ThreadedExecutor(_threadCount);
	}
	
	@Override
	public void close() throws Exception {
		_executor.terminate(_fetcherBuilder.getTimeoutInSeconds(), TimeUnit.SECONDS);
		
		super.close();
	}
	
	@Override
	public void asyncInvoke(FetchUrl url, AsyncCollector<Tuple2<CrawlStateUrl, FetchedUrl>> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);
		
		final String domainKey = url.getUrlWithoutPath();
		synchronized (_nextFetch) {
			Long nextFetchTime = _nextFetch.get(domainKey);
			long currentTime = System.currentTimeMillis();
			if ((nextFetchTime != null) && (currentTime < nextFetchTime)) {
				LOGGER.debug("Skipping (crawl-delay) " + url);
				collector.collect(skipUrl(url, nextFetchTime));
				return;
			} else {
				_nextFetch.put(domainKey, currentTime + url.getCrawlDelay());
			}
		}
		
		LOGGER.debug("Queuing for fetch: " + url);
		_executor.execute(new Runnable() {
			
			@Override
			public void run() {				
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
