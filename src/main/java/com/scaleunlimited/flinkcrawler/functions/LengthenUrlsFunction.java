package com.scaleunlimited.flinkcrawler.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings({ "serial", "unused" })
public class LengthenUrlsFunction extends RichFlatMapFunction<RawUrl, RawUrl> {

	private transient ConcurrentLinkedQueue<RawUrl> _input;
	private transient ConcurrentLinkedQueue<RawUrl> _output;
	private transient Thread _lengthenerThread;
	
	public LengthenUrlsFunction() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_input = new ConcurrentLinkedQueue<>();
		_output = new ConcurrentLinkedQueue<>();
		
		_lengthenerThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (!Thread.interrupted()) {
					if (!_input.isEmpty()) {
						// TODO lengthen the URL if the domain is a link shortener.
						RawUrl url = _input.remove();
						System.out.println("Lengthening " + url);
						_output.add(url);
					} else {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				}
			}
		}, "URL lengthener");
		
		_lengthenerThread.start();
	}
	
	@Override
	public void close() throws Exception {
		_lengthenerThread.interrupt();
		super.close();
	}
	
	@Override
	public void flatMap(RawUrl url, Collector<RawUrl> collector) throws Exception {
		if (url.isTickle()) {
			while (!_output.isEmpty()) {
				RawUrl lengthenedUrl = _output.remove();
				System.out.println("Removing URL from lengthening queue: " + lengthenedUrl);
				collector.collect(lengthenedUrl);
			}
		} else {
			// TODO block if queue is too big?
			System.out.println("Adding URL to lengthening queue: " + url);
			_input.add(url);
		}
	}

}
