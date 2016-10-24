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

import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings({ "serial", "unused" })
public class FetchUrlsFunction extends RichCoFlatMapFunction<FetchUrl, Tuple0, FetchedUrl> {

	private static final int MIN_THREAD_COUNT = 10;
	private static final int MAX_THREAD_COUNT = 100;
	
	private static final int MAX_QUEUED_URLS = 1000;
	
	private transient ConcurrentLinkedQueue<FetchedUrl> _output;
	private transient ThreadPoolExecutor _executor;
	
	public FetchUrlsFunction() {
		// TODO Auto-generated constructor stub
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
		// TODO flush out everything from the executor service.
		super.close();
	}
	
	@Override
	public void flatMap1(final FetchUrl url, Collector<FetchedUrl> collector) throws Exception {
		System.out.println("Adding URL to fetch queue: " + url);
		_executor.execute(new Runnable() {
			
			@Override
			public void run() {
				// TODO fetch the URL.
				System.out.println("Fetching " + url);
				Map<String, List<String>> headers = new HashMap<>();
				byte[] content = new byte[0];
				FetchedUrl result = new FetchedUrl(url.getUrl(), url.getUrl(), System.currentTimeMillis(), headers, content, "text/html", 0);
				_output.add(result);
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
