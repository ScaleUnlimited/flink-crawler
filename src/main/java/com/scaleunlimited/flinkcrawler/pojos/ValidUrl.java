package com.scaleunlimited.flinkcrawler.pojos;

import java.net.MalformedURLException;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.domains.EffectiveTldFinder;

@SuppressWarnings("serial")
public class ValidUrl extends BaseUrl {
    private static final Logger LOGGER = LoggerFactory.getLogger(ValidUrl.class);

    private transient String _protocol;
    private transient String _hostname;
    private transient String _pld;
    private transient int _port;
    private transient String _path;
    private transient String _query;

    public ValidUrl() {
        super();
    }

    public ValidUrl(String urlAsString) throws MalformedURLException {
        super();

        setUrl(urlAsString);
    }

    public ValidUrl(ValidUrl base) {
        super();

        setFrom(base);
    }

    protected ValidUrl(BaseUrl base) {
        super(base);

        try {
            parseUrl();
        } catch (MalformedURLException e) {
            throw new RuntimeException("Impossible exception", e);
        }
    }

    @Override
    public void setUrl(String urlAsString) throws MalformedURLException {
        super.setUrl(urlAsString);

        parseUrl();
    }

    private void parseUrl() throws MalformedURLException {
        String urlAsString = getUrl();
        if (urlAsString == null) {
            _protocol = null;
            _hostname = null;
            _port = -1;
            _path = null;
            _query = null;
            _pld = null;
        } else if (_pld == null) {
            URL url = new URL(urlAsString);

            _protocol = url.getProtocol();
            _hostname = url.getHost();
            _port = url.getPort();
            if (_port == url.getDefaultPort()) {
                _port = -1;
            }

            _path = url.getPath();
            _query = url.getQuery();

            _pld = extractPld(_hostname);
        }
    }

    private void checkUrl() {
        try {
            parseUrl();
        } catch (MalformedURLException e) {
            throw new RuntimeException("Impossible error", e);
        }
    }

    public String getProtocol() {
        checkUrl();

        return _protocol;
    }

    public String getHostname() {
        checkUrl();

        return _hostname;
    }

    public String getPld() {
        checkUrl();

        return _pld;
    }

    public int getPort() {
        checkUrl();

        return _port;
    }

    public String getPath() {
        checkUrl();

        return _path;
    }

    public String getQuery() {
        checkUrl();

        return _query;
    }

    /**
     * @return Portion of URL with protocol, domain, and any non-standard port
     */
    public String getUrlWithoutPath() {
        int port = getPort();
        if (port == -1) {
            return String.format("%s://%s", getProtocol(), getHostname());
        } else {
            return String.format("%s://%s:%d", getProtocol(), getHostname(), port);
        }
    }

    public void clear() {
        try {
            setUrl(null);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Impossible exception!", e);
        }
    }

    public void setFrom(ValidUrl url) {
        super.setFrom(url);

        _protocol = url._protocol;
        _hostname = url._hostname;
        _pld = url._pld;
        _port = url._port;
        _path = url._path;
        _query = url._query;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    private static String extractPld(String hostname) {
        // Use support in EffectiveTldFinder
        String result = EffectiveTldFinder.getAssignedDomain(hostname, true);
        if (result == null) {
            LOGGER.trace("Hostname {} isn't a valid FQDN", hostname);
            return hostname;
        } else {
            return result;
        }
    }
}
