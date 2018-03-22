package com.scaleunlimited.flinkcrawler.pojos;

import crawlercommons.util.Headers;

@SuppressWarnings("serial")
public class FetchResultUrl extends ValidUrl {

    private FetchStatus _status;
    private long _statusTime;
    private String _fetchedUrl;
    private Headers _headers;
    private byte[] _content;
    private String _contentType;
    private int _responseRate;
    private long _nextFetchTime = 0L;

    public FetchResultUrl() {
        super();
    }

    public FetchResultUrl(ValidUrl url, FetchStatus status, long statusTime) {
        this(url, status, statusTime, url.getUrl(), null, null, null, 0);
    }

    public FetchResultUrl(ValidUrl url, FetchStatus status, long statusTime, String fetchedUrl,
            Headers headers, byte[] content, String contentType, int responseRate) {
        super(url);

        _status = status;
        _statusTime = statusTime;
        _fetchedUrl = fetchedUrl;
        _headers = headers;
        _content = content;
        _contentType = contentType;
        _responseRate = responseRate;

        // TODO do we need redirects or new baseUrl ?
    }

    public FetchStatus getStatus() {
        return _status;
    }

    public void setStatus(FetchStatus status) {
        _status = status;
    }

    public long getStatusTime() {
        return _statusTime;
    }

    public void setStatusTime(long statusTime) {
        _statusTime = statusTime;
    }

    public String getFetchedUrl() {
        return _fetchedUrl;
    }

    public void setFetchedUrl(String fetchedUrl) {
        _fetchedUrl = fetchedUrl;
    }

    public Headers getHeaders() {
        return _headers;
    }

    public void setHeaders(Headers headers) {
        _headers = headers;
    }

    public byte[] getContent() {
        return _content;
    }

    public void setContent(byte[] content) {
        _content = content;
    }

    public String getContentType() {
        return _contentType;
    }

    public void setContentType(String contentType) {
        _contentType = contentType;
    }

    public int getResponseRate() {
        return _responseRate;
    }

    public void setResponseRate(int responseRate) {
        _responseRate = responseRate;
    }

    public long getNextFetchTime() {
        return _nextFetchTime;
    }

    public void setNextFetchTime(long nextFetchTime) {
        _nextFetchTime = nextFetchTime;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", getUrl(), getContentType());
    }

}
