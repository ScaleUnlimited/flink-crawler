package com.scaleunlimited.flinkcrawler.pojos;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class BaseUrl implements Serializable {
	
	// This is the full URL
	protected String _url;

	// TODO add protocol, hostname, port, path, etc as pieces.
	
	// TODO put pld field here.
	
	// TODO split up URL. So this will throw a malformed url exception
	public BaseUrl(String url) {
		_url = url;
	}
	
	// TODO rename asString()
	public String getUrl() {
		return _url;
	}

	// TODO just return pld here
	public abstract String getPartitionKey();
}
