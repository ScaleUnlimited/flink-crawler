package com.scaleunlimited.flinkcrawler.pojos;

public class ScoredUrl extends LengthenedUrl {

	private float _actualScore;

	public ScoredUrl(String url, float estimatedScore, String pld, float actualScore) {
		super(url, estimatedScore, pld);
		
		_actualScore = actualScore;
	}

	public float getActualScore() {
		return _actualScore;
	}

	public void setActualScore(float actualScore) {
		_actualScore = actualScore;
	}
	
}
