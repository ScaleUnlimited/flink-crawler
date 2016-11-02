package com.scaleunlimited.flinkcrawler.sources;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public abstract class BaseUrlSource extends RichParallelSourceFunction<RawUrl> {

}
