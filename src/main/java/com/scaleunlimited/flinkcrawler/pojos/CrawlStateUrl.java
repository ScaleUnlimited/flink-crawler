package com.scaleunlimited.flinkcrawler.pojos;

import java.net.MalformedURLException;

import com.scaleunlimited.flinkcrawler.utils.HashUtils;


/**
 * The CrawlStateUrl is the fundamental unit of state in the CrawlDB. It consists of the
 * actual URL, plus other fields necessary to handle merging of URLs and prioritizing of
 * URLs to be fetched.
 * 
 */
@SuppressWarnings("serial")
public class CrawlStateUrl extends ValidUrl {

	private FetchStatus _status;
	private long _statusTime;
	private float _score;
	private long _nextFetchTime;
	
	// What type of URL we've got. URLs in the crawlDB will only be
	// of type REGULAR, others are transient state.
	private UrlType _urlType;
	private int _id;
	
	public static CrawlStateUrl makeTicklerUrl(int id) {
		return new CrawlStateUrl(UrlType.TICKLER, id);
	}

	public static CrawlStateUrl makeDomainUrl(String domain) throws MalformedURLException {
		return new CrawlStateUrl(UrlType.DOMAIN, domain);
	}

	public CrawlStateUrl() {
		// For creating from payload
	}
	
	public CrawlStateUrl(UrlType urlType, int id) {
		_urlType = urlType;
		_id = id;
	}
	
	public CrawlStateUrl(UrlType urlType, String domain) throws MalformedURLException {
		super(new ValidUrl("http://" + domain));
		_urlType = urlType;
	}
	
	public CrawlStateUrl(FetchUrl url, FetchStatus status, long nextFetchTime) {
		this(url, status, System.currentTimeMillis(), url.getScore(), nextFetchTime);
	}
	
	public CrawlStateUrl(ValidUrl url, FetchStatus status, long statusTime, float score, long nextFetchTime) {
		super(url);

		_status = status;
		_score = score;
		_statusTime = statusTime;
		_nextFetchTime = nextFetchTime;
		
		_urlType = UrlType.REGULAR;
	}

	public long makeKey() {
		return HashUtils.longHash(getUrl());
	}

	/* (non-Javadoc)
	 * @see com.scaleunlimited.flinkcrawler.pojos.ValidUrl#getPartitionKey()
	 * 
	 * For special URLs, we have to return the the id, so that we can ensure
	 * every CrawlDBFunction gets a regular tickler.
	 */
	@Override
	public Integer getPartitionKey() {
		if ((_urlType == UrlType.REGULAR) || (_urlType == UrlType.DOMAIN)) {
			return super.getPartitionKey();
		} else {
			return _id;
		}
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

	public UrlType getUrlType() {
		return _urlType;
	}
	
	public void setUrlType(UrlType urlType) {
		_urlType = urlType;
	}
	
    /**
     * Set all fields from url
     * 
     * @param url
     */
    public void setFrom(CrawlStateUrl url) {
        super.setFrom(url);
        
        _id = url._id;
        _nextFetchTime = url._nextFetchTime;
        _score = url._score;
        _status = url._status;
        _statusTime = url._statusTime;
        _urlType = url._urlType;
    }

	@Override
	public String toString() {
		// TODO add more fields to the response.
		if (_urlType == UrlType.REGULAR) {
			return String.format("%s (%s)", getUrl(), _status);
		} else {
			return String.format("%s (%d)", _urlType, _id);
		}
	}

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + _id;
        result = prime * result + (int) (_nextFetchTime ^ (_nextFetchTime >>> 32));
        result = prime * result + Float.floatToIntBits(_score);
        result = prime * result + ((_status == null) ? 0 : _status.hashCode());
        result = prime * result + (int) (_statusTime ^ (_statusTime >>> 32));
        result = prime * result + ((_urlType == null) ? 0 : _urlType.hashCode());
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
        if (_id != other._id)
            return false;
        if (_nextFetchTime != other._nextFetchTime)
            return false;
        if (Float.floatToIntBits(_score) != Float.floatToIntBits(other._score))
            return false;
        if (_status != other._status)
            return false;
        if (_statusTime != other._statusTime)
            return false;
        if (_urlType != other._urlType)
            return false;
        return true;
    }

}