package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;

@SuppressWarnings("serial")
public class NormalizeUrlsFunction extends BaseFlatMapFunction<RawUrl, RawUrl> {

    private final BaseUrlNormalizer _normalizer;

	public NormalizeUrlsFunction() {
        this(new SimpleUrlNormalizer());
	}

	public NormalizeUrlsFunction(BaseUrlNormalizer normalizer) {
		super();
		
        _normalizer = normalizer;
	}

	@Override
	public void flatMap(RawUrl url, Collector<RawUrl> collector) throws Exception {
		record(this.getClass(), url);

		String rawUrl = url.getUrl();
		String normalizedUrl = _normalizer.normalize(rawUrl);

		RawUrl output = new RawUrl(normalizedUrl, url.getScore());
		collector.collect(output);
	}

}
