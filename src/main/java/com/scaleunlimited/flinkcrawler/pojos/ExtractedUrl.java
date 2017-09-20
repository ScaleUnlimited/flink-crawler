package com.scaleunlimited.flinkcrawler.pojos;



@SuppressWarnings("serial")
public class ExtractedUrl extends BaseUrl {

    private String _anchorText;
    private String _relAttributes;
    private float _score;
    
    public ExtractedUrl(String url) {
    	this(url, null, null);
    }
    
    public ExtractedUrl(String url, String anchorText, String relAttributes) {
    	this(url, anchorText, relAttributes, RawUrl.DEFAULT_SCORE);
    }
    
    public ExtractedUrl(String url, String anchorText, String relAttributes, float score) {
    	super(url);
    	
        _anchorText = anchorText;
        _relAttributes = relAttributes;
        _score = score;
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

	public float getScore() {
		return _score;
	}
	
	public void setScore(float score) {
		_score = score;
	}
}
