package com.scaleunlimited.flinkcrawler.sources;

import java.util.Arrays;
import java.util.LinkedList;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class SeedUrlSource implements SourceFunction<RawUrl> {

	private boolean _keepRunning = false;
	private LinkedList<RawUrl> _urls;
	
	public SeedUrlSource(float estimatedScore, String... rawUrls) {
		_urls = new LinkedList<>();
		for (String rawUrl : rawUrls) {
			_urls.push(new RawUrl(rawUrl, estimatedScore));
		}
	}

	public SeedUrlSource(RawUrl... rawUrls) {
		_urls = new LinkedList<>();
		_urls.addAll(Arrays.asList(rawUrls));
	}

	@Override
	public void cancel() {
		_keepRunning = false;
	}

	@Override
	public void run(SourceContext<RawUrl> context) throws Exception {
		_keepRunning = true;
		
		while (_keepRunning) {
			if (!_urls.isEmpty()) {
				context.collect(_urls.pop());
			} else {
				Thread.sleep(1000L);
			}
		}
	}

}
