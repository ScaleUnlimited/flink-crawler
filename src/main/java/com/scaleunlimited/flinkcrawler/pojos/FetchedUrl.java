package com.scaleunlimited.flinkcrawler.pojos;

import java.util.List;
import java.util.Map;

import org.apache.tika.metadata.Metadata;


public class FetchedUrl {

	private String _baseUrl;
	private String _fetchedUrl;
	private long _fetchTime;
	private Metadata _headers;
	private byte[] _content;
	private String _contentType;
	private int _responseRate;

	
	public FetchedUrl(String baseUrl, String fetchedUrl, long fetchTime, Metadata headers,
			byte[] content, String contentType, int responseRate) {
		_baseUrl = baseUrl;
		_fetchedUrl= fetchedUrl;
		_fetchTime = fetchTime;
		_headers = headers;
		_content = content;
		_contentType = contentType;
		_responseRate = responseRate;
		
		// TODO do we need redirects or new baseUrl ?
	}


	public String getBaseUrl() {
		return _baseUrl;
	}


	public void setBaseUrl(String baseUrl) {
		_baseUrl = baseUrl;
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


	public Metadata getHeaders() {
		return _headers;
	}


	public void setHeaders(Metadata headers) {
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

	
	
}
