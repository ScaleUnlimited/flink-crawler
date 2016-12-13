package com.scaleunlimited.flinkcrawler.pojos;

import java.net.MalformedURLException;
import java.net.URL;

import crawlercommons.url.PaidLevelDomain;


@SuppressWarnings("serial")
public class RawUrl extends BaseUrl {

	private String _url;
	private String _pld;
	private float _estimatedScore;
	
	public RawUrl() {
		super();
	}
	
	public RawUrl(String url, float estimatedScore) throws MalformedURLException {
		this(url, PaidLevelDomain.getPLD(new URL(url)), estimatedScore);
	}

	public RawUrl(String url, String pld, float estimatedScore) {
		super();
		
		_url = url;
		_pld = pld;
		_estimatedScore = estimatedScore;
	}

	@Override
	public String getPartitionKey() {
		return _pld;
	}

	public String getUrl() {
		return _url;
	}

	public void setUrl(String url) {
		_url = url;
	}

	public String getPLD() {
	    return _pld;
	}
	
	public void setPLD(String pld) {
	    _pld = pld;
	}
	
	public float getEstimatedScore() {
		return _estimatedScore;
	}

	public void setEstimatedScore(float estimatedScore) {
		_estimatedScore = estimatedScore;
	}
	
	@Override
	public String toString() {
		return _url;
	}
	
}
