package com.scaleunlimited.flinkcrawler.pojos;

public class RawUrl {

	private String _url;
	private float _estimatedScore;
	
	public RawUrl(String url, float estimatedScore) {
		_url = url;
		_estimatedScore = estimatedScore;
	}

	public String getUrl() {
		return _url;
	}

	public void setUrl(String url) {
		_url = url;
	}

	public float getEstimatedScore() {
		return _estimatedScore;
	}

	public void setEstimatedScore(float estimatedScore) {
		_estimatedScore = estimatedScore;
	}
	
}
