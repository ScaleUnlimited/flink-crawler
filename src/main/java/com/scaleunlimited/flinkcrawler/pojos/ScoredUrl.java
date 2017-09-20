package com.scaleunlimited.flinkcrawler.pojos;

@SuppressWarnings("serial")
public class ScoredUrl extends ValidUrl {

	private float _score;
	
	public ScoredUrl(ValidUrl url, float score) {
		super(url);
		
		_score = score;
	}

	public float getScore() {
		return _score;
	}

	public void setScore(float score) {
		_score = score;
	}
	
}
