package com.scaleunlimited.flinkcrawler.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.fetcher.BaseFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.FetchedResult;
import com.scaleunlimited.flinkcrawler.fetcher.HttpFetchException;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings({ "serial", "unused" })
public class FetchUrlsFunction extends RichCoFlatMapFunction<FetchUrl, Tuple0, FetchedUrl> {

	private static final int MIN_THREAD_COUNT = 10;
	private static final int MAX_THREAD_COUNT = 100;
	
	private static final int MAX_QUEUED_URLS = 1000;
	
	private BaseFetcher _fetcher;
	
	private transient ConcurrentLinkedQueue<FetchedUrl> _output;
	private transient ThreadPoolExecutor _executor;
	
	public FetchUrlsFunction(BaseFetcher fetcher) {
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
	public void flatMap1(final FetchUrl url, Collector<FetchedUrl> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);
		
		_executor.execute(new Runnable() {
			
			@Override
			public void run() {
				System.out.println("Fetching " + url);
				
				try {
					FetchedResult result = _fetcher.get(url);
					FetchedUrl fetchedUrl = new FetchedUrl(result.getBaseUrl(), result.getFetchedUrl(),
														result.getFetchTime(), result.getHeaders(), 
														result.getContent(), result.getContentType(),
														result.getResponseRate());
					
					System.out.println("Fetched " + result);
					_output.add(fetchedUrl);
				} catch (HttpFetchException e) {
					// TODO generate Tuple2 with fetch status tuple but no fetchedurl
				} catch (Exception e) {
					// TODO need to map BaseFetchException to appropriate status URL.
					throw new RuntimeException("Exception fetching " + url, e);
				}
			}
		});
	}

	@Override
	public void flatMap2(Tuple0 tickle, Collector<FetchedUrl> collector) throws Exception {
		while (!_output.isEmpty()) {
			FetchedUrl url = _output.remove();
			System.out.println("Removing URL from fetched queue: " + url);
			collector.collect(url);
		}
	}
	

}
