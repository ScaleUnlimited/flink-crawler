package com.scaleunlimited.flinkcrawler.pojos;

import java.net.MalformedURLException;

@SuppressWarnings("serial")
public class RawUrl extends BaseUrl {

	public static final float DEFAULT_SCORE = 1.0f;
	
	private float _score;
	
	public RawUrl() {
		super();
	}
	
	public RawUrl(String url) throws MalformedURLException {
		this(url, DEFAULT_SCORE);
	}
	
	public RawUrl(String url, float score) {
		super(url);
		
		setScore(score);
	}

	public RawUrl(BaseUrl url, float score) {
		super(url);
		
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
	    if (isRegular()) {
	        return getUrl();
	    } else {
	        return String.format("%s (%s)", getUrl(), getUrlType());
	    }
	}
	
	public static RawUrl makeRawTickerUrl(int maxParallelism, int parallelism, int operatorIndex) {
        return new RawUrl(BaseUrl.makeTicklerUrl(maxParallelism, parallelism, operatorIndex), 0.0f);
	}
}
