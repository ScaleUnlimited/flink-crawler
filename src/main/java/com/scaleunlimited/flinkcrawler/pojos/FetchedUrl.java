package com.scaleunlimited.flinkcrawler.pojos;

import crawlercommons.util.Headers;


@SuppressWarnings("serial")
public class FetchedUrl extends ValidUrl {

	private String _fetchedUrl;
	private long _fetchTime;
	private Headers _headers;
	private byte[] _content;
	private String _contentType;
	private int _responseRate;
	
	public FetchedUrl() {
	    super();
	}
	
	public FetchedUrl(ValidUrl url, String fetchedUrl, long fetchTime, Headers headers,
			byte[] content, String contentType, int responseRate) {
		super(url);
		
		_fetchedUrl= fetchedUrl;
		_fetchTime = fetchTime;
		_headers = headers;
		_content = content;
		_contentType = contentType;
		_responseRate = responseRate;
		
		// TODO do we need redirects or new baseUrl ?
	}

	public String getFetchedUrl() {
		return _fetchedUrl;
	}

	public void setFetchedUrl(String fetchedUrl) {
		_fetchedUrl = fetchedUrl;
	}

	public long getFetchTime() {
		return _fetchTime;
	}

	public void setFetchTime(long fetchTime) {
		_fetchTime = fetchTime;
	}


	public Headers getHeaders() {
		return _headers;
	}


	public void setHeaders(Headers headers) {
		_headers = headers;
	}


	public byte[] getContent() {
		return _content;
	}


	public void setContent(byte[] content) {
		_content = content;
	}


	public String getContentType() {
		return _contentType;
	}


	public void setContentType(String contentType) {
		_contentType = contentType;
	}


	public int getResponseRate() {
		return _responseRate;
	}


	public void setResponseRate(int responseRate) {
		_responseRate = responseRate;
	}

	@Override
	public String toString() {
		return String.format("%s (%s)", getUrl(), getContentType());
	}
	
}
