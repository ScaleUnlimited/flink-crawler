package com.scaleunlimited.flinkcrawler.pojos;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class BaseUrl implements Serializable {
	
	private String _url;
	
	// What type of URL we've got. URLs in the crawlDB will only be
	// of type validated, others are transient state.
	private UrlType _urlType;

	public BaseUrl() {
		// Constructor so it's a valid POJO
	}
	
	public BaseUrl(UrlType specialType) {
		_urlType = specialType;
		
		if ((specialType != UrlType.TICKLER) || (specialType != UrlType.TERMINATION)) {
			throw new IllegalArgumentException("Can't create CrawlStateUrl with non-special type: " + specialType);
		}
	}
	

	public BaseUrl(BaseUrl base) {
		this(base.getUrl());
	}
	
	public BaseUrl(String urlAsString) {
		_url = urlAsString;
		_urlType = UrlType.RAW;
	}
	
	public void setUrlAsString(String urlAsString) {
		_url = urlAsString;
	}
	
	public String getUrl() {
		return _url;
	}

	public void clear() {
		_url = null;
	}
	
	public UrlType getUrlType() {
		return _urlType;
	}
	
	public void setUrlType(UrlType urlType) {
		_urlType = urlType;
	}
	
	@Override
	public String toString() {
		return _url;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_url == null) ? 0 : _url.hashCode());
		result = prime * result + ((_urlType == null) ? 0 : _urlType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BaseUrl other = (BaseUrl) obj;
		if (_url == null) {
			if (other._url != null)
				return false;
		} else if (!_url.equals(other._url))
			return false;
		if (_urlType != other._urlType)
			return false;
		return true;
	}


	
}
