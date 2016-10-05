package com.scaleunlimited.flinkcrawler.pojos;


public class CrawledUrl {

	private String _url;
	private String _status;		// TODO make this an enum ?
	private String _pld;
	private float _actualScore;	// TODO do we maintain separate page and link scores ?
	private float _estimatedScore;
	private long _lastFetchedTime;
	private long _nextFetchTime;
	
	public CrawledUrl(String url, String status, String pld, float actualScore, float estimatedScore, long lastFetchedTime, long nextFetchTime) {
		_url = url;
		_status = status;
		_pld = pld;
		_actualScore = actualScore;
		_estimatedScore = estimatedScore;
		_lastFetchedTime = lastFetchedTime;
		_nextFetchTime = nextFetchTime;
	}
	
	public String getUrl() {
		return _url;
	}
	public void setUrl(String url) {
		_url = url;
	}
	
	public String getStatus() {
		return _status;
	}
	public void setStatus(String status) {
		_status = status;
	}
	
	public String getPLD() {
		return _pld;
	}
	
	public void setPLD(String pld) {
		_pld = pld;
	}
	
	public float getActualScore() {
		return _actualScore;
	}
	
	public void setActualScore(float actualScore) {
		_actualScore = actualScore;
	}
	
	public float getEstimatedScore() {
		return _estimatedScore;
	}
	
	public void setEstimatedScore(float estimatedScore) {
		_estimatedScore = estimatedScore;
	}
	
	public float getLastFetchedTime() {
		return _lastFetchedTime;
	}
	
	public void setLastFetchedTime(long lastFetchedTime) {
		_lastFetchedTime = lastFetchedTime;
	}
	
	public float getNextFetchTime() {
		return _nextFetchTime;
	}
	
	public void setNextFetchTime(long nextFetchTime) {
		_nextFetchTime = nextFetchTime;
	}
	

	
}