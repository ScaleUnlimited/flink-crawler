package com.scaleunlimited.flinkcrawler.functions;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
import com.scaleunlimited.flinkcrawler.utils.ThreadedExecutor;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings({ "serial" })
public class LengthenUrlsFunction extends RichAsyncFunction<RawUrl, RawUrl> {
	static final Logger LOGGER = LoggerFactory.getLogger(LengthenUrlsFunction.class);
	
	// FUTURE make this settable from command line
	// See https://github.com/ScaleUnlimited/flink-crawler/issues/50
	private static final int THREAD_COUNT = 100;
	
	private BaseUrlLengthener _lengthener;
	
	private transient ThreadedExecutor _executor;
	
	public LengthenUrlsFunction(BaseUrlLengthener lengthener) {
		_lengthener = lengthener;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_executor = new ThreadedExecutor(THREAD_COUNT);
	}
	
	@Override
	public void close() throws Exception {
		_executor.terminate(_lengthener.getTimeoutInSeconds(), TimeUnit.SECONDS);
		
		super.close();
	}
	
	@Override
	public void asyncInvoke(final RawUrl url, AsyncCollector<RawUrl> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);

		_executor.execute(new Runnable() {
			
			@Override
			public void run() {
				RawUrl lengthenedUrl = _lengthener.lengthen(url);
				collector.collect(Collections.singleton(lengthenedUrl));
			}
		});
	}

}
