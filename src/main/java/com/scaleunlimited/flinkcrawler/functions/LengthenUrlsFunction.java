package com.scaleunlimited.flinkcrawler.functions;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings({ "serial" })
public class LengthenUrlsFunction extends RichProcessFunction<RawUrl, RawUrl> {

	private static final int MIN_THREAD_COUNT = 10;
	private static final int MAX_THREAD_COUNT = 100;
	
	private static final int MAX_QUEUED_URLS = 1000;
	private static final long QUEUE_CHECK_DELAY = 10;
	
	private BaseUrlLengthener _lengthener;
	
	private transient ConcurrentLinkedQueue<RawUrl> _output;
	private transient ThreadPoolExecutor _executor;
	
	public LengthenUrlsFunction(BaseUrlLengthener lengthener) {
		_lengthener = lengthener;
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
	public void processElement(final RawUrl url, Context context, Collector<RawUrl> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);

		_executor.execute(new Runnable() {
			
			@Override
			public void run() {
				System.out.println("Lengthening " + url);
				_output.add(_lengthener.lengthen(url));
			}
		});
		
		// Every time we get called, we'll set up a new timer that fires
		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}
	
	@Override
	public void onTimer(long time, OnTimerContext context, Collector<RawUrl> collector) throws Exception {
		// TODO use a loop?
		if (!_output.isEmpty()) {
			RawUrl lengthenedUrl = _output.remove();
			System.out.println("Removing URL from lengthening queue: " + lengthenedUrl);
			collector.collect(lengthenedUrl);
		}
		
		context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + QUEUE_CHECK_DELAY);
	}

}
