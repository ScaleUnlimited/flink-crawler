package com.scaleunlimited.flinkcrawler.pojos;

import java.net.MalformedURLException;

@SuppressWarnings("serial")
public class RawUrl extends BaseUrl {

	private float _estimatedScore;
	
	public RawUrl() {
		super();
	}
	
	public RawUrl(String url) throws MalformedURLException {
		this(url, 0.0f);
	}
	
	public RawUrl(String url, float estimatedScore) throws MalformedURLException {
		super(url);
		
		setEstimatedScore(estimatedScore);
	}

	public RawUrl(BaseUrl url, float estimatedScore) {
		super(url.getUrl());
		
		setEstimatedScore(estimatedScore);
	}

	public float getEstimatedScore() {
		return _estimatedScore;
	}

	public void setEstimatedScore(float estimatedScore) {
		_estimatedScore = estimatedScore;
	}
	
	@Override
	public String toString() {
		return getUrl();
	}
	
}
