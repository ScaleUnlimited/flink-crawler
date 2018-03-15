package com.scaleunlimited.flinkcrawler.functions;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import com.scaleunlimited.flinkcrawler.pojos.BaseUrl;
import com.scaleunlimited.flinkcrawler.utils.ThreadedExecutor;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings("serial")
public abstract class BaseAsyncFunction<IN, OUT> extends RichAsyncFunction<IN, OUT> {

	private final int _threadCount;
	private final int _timeoutInSeconds;
	
	protected transient int _parallelism;
	protected transient int _partition;
	protected transient ThreadedExecutor _executor;

	public BaseAsyncFunction(int threadCount, int timeoutInSeconds) {
		super();
		
		_threadCount = threadCount;
		_timeoutInSeconds = timeoutInSeconds;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		RuntimeContext context = getRuntimeContext();
		_parallelism = context.getNumberOfParallelSubtasks();
		_partition = context.getIndexOfThisSubtask() + 1;
		
		_executor = new ThreadedExecutor("Flink-crawler-" + context.getTaskName(), _threadCount);
	}
	
	@Override
	public void close() throws Exception {
	    if (_executor != null) {
	        try {
	            _executor.terminate(_timeoutInSeconds, TimeUnit.SECONDS);
	        } catch (InterruptedException e) {
	            // TODO we have an issue where the TaskManager thread that's
	            // calling us gets interrupted right away, and that in turn
	            // triggers this exception.
	        }
	    }
	    
		super.close();
	}
	
	protected void record(Class<?> clazz, BaseUrl url, String... metaData) {
		UrlLogger.record(clazz, _partition, _parallelism, url, metaData);
	}
	
	protected void record(Class<?> clazz, BaseUrl url) {
		UrlLogger.record(clazz, _partition, _parallelism, url);
	}
	
}
