package com.scaleunlimited.flinkcrawler.pojos;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class BaseUrl implements Serializable {
	
	protected String _url;

	public BaseUrl(String url) {
		_url = url;
	}
	
	public String getUrl() {
		return _url;
	}

	public abstract String getPartitionKey();
}
