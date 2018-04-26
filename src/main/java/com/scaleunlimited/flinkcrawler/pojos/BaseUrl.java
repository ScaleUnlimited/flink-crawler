package com.scaleunlimited.flinkcrawler.pojos;

import java.io.Serializable;
import java.net.MalformedURLException;

import com.scaleunlimited.flinkcrawler.utils.FlinkUtils;

@SuppressWarnings("serial")
public abstract class BaseUrl implements Serializable {

    private static String TICKLER_URL_FORMAT = "flickcrawler-tickler-url-%d.com";
    private static String TERMINATE_URL_FORMAT = "flickcrawler-terminate-url-%d.com";

    private String _url;
    private UrlType _urlType;

    public BaseUrl() {
        _urlType = UrlType.REGULAR;
    }

    public BaseUrl(BaseUrl base) {
        this(base.getUrl(), base.getUrlType());
    }

    public BaseUrl(String urlAsString) {
        this(urlAsString, UrlType.REGULAR);
    }

    protected BaseUrl(String urlAsString, UrlType urlType) {
        _url = urlAsString;
        _urlType = urlType;
    }

    public void setUrl(String url) throws MalformedURLException {
        _url = url;
    }

    public String getUrl() {
        return _url;
    }

    public void setUrlType(UrlType type) {
        _urlType = type;
    }

    public UrlType getUrlType() {
        return _urlType;
    }

    public boolean isRegular() {
        return _urlType == UrlType.REGULAR;
    }

    public void clear() {
        _url = null;
        _urlType = UrlType.REGULAR;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", _url, _urlType);
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

    public void setFrom(BaseUrl url) {
        _url = url._url;
        _urlType = url._urlType;
    }

    public static BaseUrl makeTicklerUrl(int maxParallelism, int parallelism, int operatorIndex) {
        return new BaseUrl("http://" + FlinkUtils.makeKeyForOperatorIndex(TICKLER_URL_FORMAT, maxParallelism,
                parallelism, operatorIndex), UrlType.TICKLER) {
        };
    }

    public static BaseUrl makeTerminateUrl(int maxParallelism, int parallelism, int operatorIndex) {
        return new BaseUrl("http://" + FlinkUtils.makeKeyForOperatorIndex(TERMINATE_URL_FORMAT, maxParallelism,
                parallelism, operatorIndex), UrlType.TERMINATION) {
        };
    }

}
