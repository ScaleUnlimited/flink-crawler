package com.scaleunlimited.flinkcrawler.pojos;

import java.net.MalformedURLException;
import java.net.URL;

import crawlercommons.url.PaidLevelDomain;


@SuppressWarnings("serial")
public class ExtractedUrl extends BaseUrl {

    private String _toUrl;
    private String _pld;
    private String _anchorText;
    private String _relAttributes;
    // TODO does an outlink get an estimated score to begin with ?

    public ExtractedUrl(String toUrl, String anchorText, String relAttributes) throws MalformedURLException {
    	super();
    	
        _toUrl = toUrl;
        _anchorText = anchorText;
        _relAttributes = relAttributes;
        
		_pld = PaidLevelDomain.getPLD(new URL(toUrl));
    }
    
	@Override
	public String getPartitionKey() {
		return _pld;
	}

    public String getToUrl() {
        return _toUrl;
    }

    public void setToUrl(String toUrl) {
    	_toUrl = toUrl;
    }
    
    public String getAnchorText() {
        return _anchorText;
    }

    public void setAnchorText(String anchorText) {
        _anchorText = anchorText;
    }

    public String getRelAttributes() {
        return _relAttributes;
    }

    public void setRelAttributes(String relAttributes) {
        _relAttributes = relAttributes;
    }

    @Override
    public String toString() {
        // TODO add more fields
        return _toUrl;
    }
}
