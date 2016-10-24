package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;

@SuppressWarnings("serial")
public class NormalizeUrlsFunction extends RichFlatMapFunction<RawUrl, RawUrl> {

    private final BaseUrlNormalizer _normalizer;

	public NormalizeUrlsFunction() {
        this(new SimpleUrlNormalizer());
	}

	public NormalizeUrlsFunction(BaseUrlNormalizer normalizer) {
        _normalizer = normalizer;
	}

	@Override
	public void flatMap(RawUrl input, Collector<RawUrl> collector) throws Exception {
		_normalizer.normalize(input.getUrl());
		RawUrl output = new RawUrl(	_normalizer.normalize(input.getUrl()),
									input.getEstimatedScore());
		collector.collect(output);
	}

}
