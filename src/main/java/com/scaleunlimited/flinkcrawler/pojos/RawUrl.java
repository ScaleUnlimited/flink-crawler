package com.scaleunlimited.flinkcrawler.pojos;

import java.net.MalformedURLException;

@SuppressWarnings("serial")
public class RawUrl extends BaseUrl {

	private float _score;
	
	public RawUrl() {
		super();
	}
	
	public RawUrl(String url) throws MalformedURLException {
		this(url, 0.0f);
	}
	
	public RawUrl(String url, float score) throws MalformedURLException {
		super(url);
		
		setScore(score);
	}

	public RawUrl(BaseUrl url, float score) {
		super(url.getUrl());
		
		setScore(score);
	}

	public float getScore() {
		return _score;
	}

	public void setScore(float score) {
		_score = score;
	}
	
	@Override
	public String toString() {
		return getUrl();
	}
	
}
