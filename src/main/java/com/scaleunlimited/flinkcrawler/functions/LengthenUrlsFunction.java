package com.scaleunlimited.flinkcrawler.functions;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings({ "serial" })
public class LengthenUrlsFunction extends RichAsyncFunction<RawUrl, RawUrl> {
	static final Logger LOGGER = LoggerFactory.getLogger(LengthenUrlsFunction.class);
	
	private static final int MIN_THREAD_COUNT = 10;
	private static final int MAX_THREAD_COUNT = 100;
	
	public static final int QUEUE_SIZE = 1000;
	
	private BaseUrlLengthener _lengthener;
	
	private transient ThreadPoolExecutor _executor;
	
	public LengthenUrlsFunction(BaseUrlLengthener lengthener) {
		_lengthener = lengthener;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
		_executor = new ThreadPoolExecutor(MIN_THREAD_COUNT, MAX_THREAD_COUNT, _lengthener.getTimeoutInSeconds(), TimeUnit.SECONDS, workQueue, new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	@Override
	public void close() throws Exception {
		_executor.shutdown();
		
		if (!_executor.awaitTermination(_lengthener.getTimeoutInSeconds(), TimeUnit.SECONDS)) {
			// TODO handle timeout.
		}
		
		super.close();
	}
	
	@Override
	public void asyncInvoke(final RawUrl url, AsyncCollector<RawUrl> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);

		_executor.execute(new Runnable() {
			
			@Override
			public void run() {
				LOGGER.debug("Lengthening " + url);
				RawUrl lengthenedUrl = _lengthener.lengthen(url);
				LOGGER.debug("Emitting lengthened url " + url);
				collector.collect(Collections.singleton(lengthenedUrl));
			}
		});
	}

}
