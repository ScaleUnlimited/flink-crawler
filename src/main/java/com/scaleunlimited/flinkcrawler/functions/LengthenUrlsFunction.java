package com.scaleunlimited.flinkcrawler.functions;

import java.util.ArrayList;
import java.util.List;
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

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings({ "serial", "unused" })
public class LengthenUrlsFunction extends RichCoFlatMapFunction<RawUrl, Tuple0, RawUrl> {

	private static final int MIN_THREAD_COUNT = 10;
	private static final int MAX_THREAD_COUNT = 100;
	
	private static final int MAX_QUEUED_URLS = 1000;
	
	private transient ConcurrentLinkedQueue<RawUrl> _output;
	private transient ThreadPoolExecutor _executor;
	
	public LengthenUrlsFunction() {
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
		_executor.shutdown();
		
		// TODO get timeout from lengthener service
		if (!_executor.awaitTermination(1, TimeUnit.SECONDS)) {
			// TODO handle timeout.
		}
		
		super.close();
	}
	
	@Override
	public void flatMap1(final RawUrl url, Collector<RawUrl> collector) throws Exception {
		System.out.println("Adding URL to lengthening queue: " + url);
		_executor.execute(new Runnable() {
			
			@Override
			public void run() {
				// TODO lengthen the URL if the domain is a link shortener.
				System.out.println("Lengthening " + url);
				_output.add(url);
			}
		});
	}

	@Override
	public void flatMap2(Tuple0 tickle, Collector<RawUrl> collector) throws Exception {
		while (!_output.isEmpty()) {
			RawUrl lengthenedUrl = _output.remove();
			System.out.println("Removing URL from lengthening queue: " + lengthenedUrl);
			collector.collect(lengthenedUrl);
		}
	}
	

}
