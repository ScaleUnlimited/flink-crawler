package com.scaleunlimited.flinkcrawler.pojos;



@SuppressWarnings("serial")
public class ExtractedUrl extends BaseUrl {

    private String _anchorText;
    private String _relAttributes;
    // TODO does an outlink get an estimated score to begin with ?

    public ExtractedUrl(String url, String anchorText, String relAttributes) {
    	super(url);
    	
        _anchorText = anchorText;
        _relAttributes = relAttributes;
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
        return super.toString();
    }
}
