package com.scaleunlimited.flinkcrawler.pojos;

@SuppressWarnings("serial")
public class CrawledUrl extends ValidUrl {

    private String _status; // TODO make this an enum ?
    private float _actualScore; // TODO do we maintain separate page and link scores ?
    private float _estimatedScore;
    private long _lastFetchedTime;
    private long _nextFetchTime;

    public CrawledUrl() {
        super();
    }

    public CrawledUrl(ValidUrl url, String status, float actualScore, float estimatedScore,
            long lastFetchedTime, long nextFetchTime) {
        super(url);

        _status = status;
        _actualScore = actualScore;
        _estimatedScore = estimatedScore;
        _lastFetchedTime = lastFetchedTime;
        _nextFetchTime = nextFetchTime;
    }

    public String getStatus() {
        return _status;
    }

    public void setStatus(String status) {
        _status = status;
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