package com.scaleunlimited.flinkcrawler.sources;

import java.util.Arrays;
import java.util.LinkedList;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class SeedUrlSource implements ParallelSourceFunction<RawUrl> {

	private static final RawUrl TICKLE_URL = new RawUrl(true);
	
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
				// TODO With parallelism, we only want to emit URLs that are appropriate for us, versus
				// doing a broadcast of every URL.
				context.collect(_urls.pop());
			} else {
				context.collect(TICKLE_URL);
				Thread.sleep(100L);
			}
		}
	}

}
