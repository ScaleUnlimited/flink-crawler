package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.java.functions.KeySelector;

import com.scaleunlimited.flinkcrawler.pojos.BaseUrl;

@SuppressWarnings("serial")
public class UrlKeySelector<T extends BaseUrl> implements KeySelector<T, String> {

	@Override
	public String getKey(T url) throws Exception {
		return url.getPartitionKey();
	}

}
