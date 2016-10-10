package com.scaleunlimited.flinkcrawler.pojos;


@SuppressWarnings("serial")
public class RawUrl extends BaseUrl {

	private String _url;
	private float _estimatedScore;
	
	public RawUrl(String url, float estimatedScore) {
		super();
		
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
	
	@Override
	public String toString() {
		return _url;
	}
	
}
