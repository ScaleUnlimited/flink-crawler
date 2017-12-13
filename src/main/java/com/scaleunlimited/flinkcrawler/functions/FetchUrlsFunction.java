package com.scaleunlimited.flinkcrawler.functions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.metrics.CrawlerMetrics;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.utils.ExceptionUtils;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;


@SuppressWarnings("serial")
public class FetchUrlsFunction extends BaseAsyncFunction<FetchUrl, Tuple2<CrawlStateUrl, FetchedUrl>> {
    static final Logger LOGGER = LoggerFactory.getLogger(FetchUrlsFunction.class);
    
	public static final int DEFAULT_THREAD_COUNT = 100;
	
	private BaseHttpFetcherBuilder _fetcherBuilder;
	private BaseHttpFetcher _fetcher;
	
	// TODO use native String->long map
	private transient Map<String, Long> _nextFetch;

	/**
	 * Returns a Tuple2 of the CrawlStateUrl and FetchedUrl. In the case of an error while fetching
	 * the FetchedUrl is set to null.
	 * @param fetcherBuider
	 */
	public FetchUrlsFunction(BaseHttpFetcherBuilder fetcherBuilder) {
		super(fetcherBuilder.getMaxParallelFetches(), fetcherBuilder.getTimeoutInSeconds());
		
		_fetcherBuilder = fetcherBuilder;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		getRuntimeContext().getMetricGroup().gauge(
				CrawlerMetrics.GAUGE_URLS_CURRENTLY_BEING_FETCHED.toString(),
				new Gauge<Integer>() {
					@Override
					public Integer getValue() {
						if (_executor != null) {
							return _executor.getActiveCount();
						}
						return 0;
					}
				});
		 
		_fetcher = _fetcherBuilder.build();
		_nextFetch = new HashMap<>();
	}
	
	@Override
	public void asyncInvoke(FetchUrl url, AsyncCollector<Tuple2<CrawlStateUrl, FetchedUrl>> collector) throws Exception {
		record(this.getClass(), url);
		
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
		
		LOGGER.debug("Queueing for fetch: " + url);
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
					
					
					// If we got an error, put null in for fetchedUrl so we don't try to process it downstream.
					if (result.getStatusCode() != HttpStatus.SC_OK) {
						LOGGER.debug(String.format("Failed to fetch '%s' (%d)", result.getFetchedUrl(), result.getStatusCode()));
						FetchStatus fetchStatus = ExceptionUtils.mapHttpStatusToFetchStatus(result.getStatusCode());
						collector.collect(Collections.singleton(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url, fetchStatus, 0, System.currentTimeMillis(), 0L), null)));
						LOGGER.debug(String.format("Forwarded failed URL to update status: '%s'", result.getFetchedUrl()));
					} else {
						LOGGER.debug(String.format("Fetched %d bytes from '%s'", result.getContentLength(), result.getFetchedUrl()));
						collector.collect(Collections.singleton(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url, FetchStatus.FETCHED, 0, fetchedUrl.getFetchTime(), 0L), fetchedUrl)));
						LOGGER.debug(String.format("Forwarded fetched URL to be parsed: '%s'", result.getFetchedUrl()));
					}
				} catch (Exception e) {
					LOGGER.debug(String.format("Failed to fetch '%s' due to %s", url, e.getMessage()));
					
					if (e instanceof BaseFetchException) {
						collector.collect(Collections.singleton(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url, ExceptionUtils.mapExceptionToFetchStatus(e), 0, System.currentTimeMillis(), 0L), null)));
						LOGGER.debug(String.format("Forwarded exception URL to update status: '%s'", url));
					} else {
						throw new RuntimeException("Exception fetching " + url, e);
					}
				} catch (Throwable t) {
					LOGGER.error(String.format("Serious error trying to fetch '%s' due to %s", url, t.getMessage()), t);
					throw new RuntimeException(t);
				}
			}
		});
	}

	private Collection<Tuple2<CrawlStateUrl, FetchedUrl>> skipUrl(FetchUrl url, Long nextFetchTime) {
		return Collections.singleton(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url, FetchStatus.SKIPPED_CRAWLDELAY, nextFetchTime), null));
	}
	
}
