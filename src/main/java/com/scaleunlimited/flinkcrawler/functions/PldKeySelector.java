package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.java.functions.KeySelector;

import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;

@SuppressWarnings("serial")
public class PldKeySelector<T extends ValidUrl> implements KeySelector<T, Integer> {

	@Override
	public Integer getKey(T url) throws Exception {
		return url.getPartitionKey();
	}

}
