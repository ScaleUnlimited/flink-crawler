package com.scaleunlimited.flinkcrawler.pojos;

@SuppressWarnings("serial")
public class ScoredUrl extends ValidUrl {

	private float _actualScore;
	private float _estimatedScore;
	
	public ScoredUrl(ValidUrl url, float estimatedScore, float actualScore) {
		super(url);
		
		_estimatedScore = estimatedScore;
		_actualScore = actualScore;
	}

	public float getEstimatedScore() {
		return _estimatedScore;
	}

	public void setEstimatedScore(float estimatedScore) {
		_estimatedScore = estimatedScore;
	}
	
	public float getActualScore() {
		return _actualScore;
	}

	public void setActualScore(float actualScore) {
		_actualScore = actualScore;
	}
	
}
