package com.scaleunlimited.flinkcrawler.pojos;

import java.net.MalformedURLException;

import com.scaleunlimited.flinkcrawler.utils.HashUtils;

/**
 * The CrawlStateUrl is the fundamental unit of state in the CrawlDB. It consists of the actual URL, plus other fields
 * necessary to handle merging of URLs and prioritizing of URLs to be fetched.
 * 
 */
@SuppressWarnings("serial")
public class CrawlStateUrl extends ValidUrl {

    private FetchStatus _status;
    private FetchStatus _previousStatus;
    private long _statusTime;
    private float _score = 0.0f;
    private long _nextFetchTime = 0L;

    public CrawlStateUrl() {
        // So it's a valid POJO for Flink.
    }

    public CrawlStateUrl(RawUrl url) {
        this(new ValidUrl(url), FetchStatus.UNFETCHED, System.currentTimeMillis());
    }
    
    public CrawlStateUrl(FetchResultUrl fetchedUrl) {
        this(fetchedUrl, fetchedUrl.getStatus(), fetchedUrl.getStatusTime());
        setNextFetchTime(fetchedUrl.getNextFetchTime());
    }

    public CrawlStateUrl(ValidUrl url, FetchStatus status, long statusTime) {
        super(url);

        _status = status;
        _statusTime = statusTime;
    }

    public long makeKey() {
        return HashUtils.longHash(getUrl());
    }

    public FetchStatus getStatus() {
        return _status;
    }

    public void setStatus(FetchStatus status) {
        _status = status;
    }

    public float getScore() {
        return _score;
    }

    public void setScore(float score) {
        _score = score;
    }

    public long getStatusTime() {
        return _statusTime;
    }

    public void setStatusTime(long statusTime) {
        _statusTime = statusTime;
    }

    public long getNextFetchTime() {
        return _nextFetchTime;
    }

    public void setNextFetchTime(long nextFetchTime) {
        _nextFetchTime = nextFetchTime;
    }

    /**
     * Set all fields from url
     * 
     * @param url
     */
    public void setFrom(CrawlStateUrl url) {
        super.setFrom(url);

        _nextFetchTime = url._nextFetchTime;
        _score = url._score;
        _status = url._status;
        _statusTime = url._statusTime;
    }

    @Override
    public String toString() {
        // TODO add more fields to the response.
        if (getUrlType() == UrlType.REGULAR) {
            return String.format("%s (%s at %d)", getUrl(), _status, _statusTime);
        } else {
            return String.format("%s", getUrlType());
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (int) (_nextFetchTime ^ (_nextFetchTime >>> 32));
        result = prime * result + Float.floatToIntBits(_score);
        result = prime * result + ((_status == null) ? 0 : _status.hashCode());
        result = prime * result + (int) (_statusTime ^ (_statusTime >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        CrawlStateUrl other = (CrawlStateUrl) obj;
        if (_nextFetchTime != other._nextFetchTime)
            return false;
        if (Float.floatToIntBits(_score) != Float.floatToIntBits(other._score))
            return false;
        if (_status != other._status)
            return false;
        if (_statusTime != other._statusTime)
            return false;
        return true;
    }

    public static CrawlStateUrl makeTicklerUrl(int maxParallelism, int parallelism,
            int operatorIndex) {
        return new CrawlStateUrl(ValidUrl.makeTickerUrl(maxParallelism, parallelism, operatorIndex),
                FetchStatus.UNFETCHED, System.currentTimeMillis());
    }

    public static CrawlStateUrl makeDomainUrl(String domain) throws MalformedURLException {
        return new CrawlStateUrl(ValidUrl.makeDomainUrl(domain), FetchStatus.UNFETCHED,
                System.currentTimeMillis());
    }

    public static CrawlStateUrl makeTerminateUrl(int maxParallelism, int parallelism,
            int operatorIndex) {
        return new CrawlStateUrl(
                ValidUrl.makeTerminateUrl(maxParallelism, parallelism, operatorIndex),
                FetchStatus.UNFETCHED, System.currentTimeMillis());
    }

}