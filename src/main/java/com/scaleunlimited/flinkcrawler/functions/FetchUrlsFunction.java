package com.scaleunlimited.flinkcrawler.functions;

import com.scaleunlimited.flinkcrawler.utils.ExceptionUtils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.HttpFetchException;
import crawlercommons.fetcher.http.BaseHttpFetcher;



@SuppressWarnings("serial")
public class FetchUrlsFunction extends RichProcessFunction<FetchUrl, Tuple2<CrawlStateUrl, FetchedUrl>> {
    static final Logger LOGGER = LoggerFactory.getLogger(FetchUrlsFunction.class);
    
	private static final int MIN_THREAD_COUNT = 10;
	private static final int MAX_THREAD_COUNT = 100;
	
	private static final int MAX_QUEUED_URLS = 1000;
	
	// TODO pick good time for this
	private static final long QUEUE_CHECK_DELAY = 10;

	private BaseHttpFetcher _fetcher;
	
	private transient ConcurrentLinkedQueue<Tuple2<CrawlStateUrl, FetchedUrl>> _output;
	private transient ThreadPoolExecutor _executor;
	
	/**
	 * Returns a Tuple2 of the CrawlStateUrl and FetchedUrl. In the case of an error while fetching
	 * the FetchedUrl is set to null.
	 * @param fetcher
	 */
	public FetchUrlsFunction(BaseHttpFetcher fetcher) {
		_fetcher = fetcher;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_output = new ConcurrentLinkedQueue<>();
		
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
	public void processElement(final FetchUrl url, Context context, Collector<Tuple2<CrawlStateUrl, FetchedUrl>> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);
		
		_executor.execute(new Runnable() {
			
			@Override
			public void run() {
				LOGGER.debug("Fetching " + url);
				
				try {
					FetchedResult result = _fetcher.get(url.getUrl(), null);
					FetchedUrl fetchedUrl = new FetchedUrl(result.getBaseUrl(), result.getFetchedUrl(),
														result.getFetchTime(), result.getHeaders(), 
														result.getContent(), result.getContentType(),
														result.getResponseRate());
					
					LOGGER.info("Fetched " + result);
					_output.add(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url.getUrl(), FetchStatus.FETCHED, url.getPLD(), 0, 0, fetchedUrl.getFetchTime(), 0L), fetchedUrl));
				} catch (HttpFetchException e) {
					// Generate Tuple2 with fetch status tuple but no FetchedUrl
					_output.add(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url.getUrl(), ExceptionUtils.mapToFetchStatus(e), url.getPLD(), 0, 0, System.currentTimeMillis(), 0L), null));
				} catch (Exception e) {
					if (e instanceof BaseFetchException) {
						BaseFetchException bfe = (BaseFetchException)e;
						_output.add(new Tuple2<CrawlStateUrl, FetchedUrl>(new CrawlStateUrl(url.getUrl(), ExceptionUtils.mapToFetchStatus(e), url.getPLD(), 0, 0, System.currentTimeMillis(), 0L), null));
					} else {
						throw new RuntimeException("Exception fetching " + url, e);
					}

				}
			}
		});
		
		// Every time we get called, we'll set up a new timer that fires
		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}

	@Override
	public void onTimer(long time, OnTimerContext context, Collector<Tuple2<CrawlStateUrl, FetchedUrl>> collector) throws Exception {
		// TODO use a loop?
		
		if (!_output.isEmpty()) {
			Tuple2<CrawlStateUrl,FetchedUrl> url = _output.remove();
			LOGGER.debug("Removing URL from fetched queue: " + url);
			collector.collect(url);
		}
		
		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}
	

}
