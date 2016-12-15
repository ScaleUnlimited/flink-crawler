package com.scaleunlimited.flinkcrawler.pojos;

@SuppressWarnings("serial")
public class ScoredUrl extends RawUrl {

	private float _actualScore;

	public ScoredUrl(String url, String pld, float estimatedScore, float actualScore) {
		super(url, pld, estimatedScore);
		
		_actualScore = actualScore;
	}

	public float getActualScore() {
		return _actualScore;
	}

	public void setActualScore(float actualScore) {
		_actualScore = actualScore;
	}
	
}
