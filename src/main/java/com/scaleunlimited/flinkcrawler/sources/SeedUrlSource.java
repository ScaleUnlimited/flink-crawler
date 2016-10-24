package com.scaleunlimited.flinkcrawler.sources;

import java.util.LinkedList;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

/**
 * Source for seed URLs
 *
 */
@SuppressWarnings("serial")
public class SeedUrlSource extends RichParallelSourceFunction<RawUrl> {

	private RawUrl[] _unpartitioned;
	
	private transient boolean _keepRunning = false;
	private transient LinkedList<RawUrl> _urls;
	
	public SeedUrlSource(float estimatedScore, String... rawUrls) {
		_unpartitioned = new RawUrl[rawUrls.length];
		
		for (int i = 0; i < rawUrls.length; i++) {
			_unpartitioned[i] = new RawUrl(rawUrls[i], estimatedScore);
		}
	}

	public SeedUrlSource(RawUrl... rawUrls) {
		_unpartitioned = rawUrls;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_urls = new LinkedList<>();
		
		// Figure out which URLs in the full (unpartitioned) set are ones that
		// this source task should be emitting.
		RuntimeContext context = getRuntimeContext();
		int parallelism = context.getNumberOfParallelSubtasks();
		int myIndex = context.getIndexOfThisSubtask();
		
		for (int i = 0; i < _unpartitioned.length; i++) {
			if ((i % parallelism) == myIndex) {
				_urls.add(_unpartitioned[i]);
			}
		}
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
				// context.collect(TICKLE_URL);
				Thread.sleep(100L);
			}
		}
	}

}
