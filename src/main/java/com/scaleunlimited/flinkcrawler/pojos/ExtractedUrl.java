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
    	
    	// Often comes in with extra spaces/returns before the actual text.
        _anchorText = anchorText.trim();
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
    	return String.format("[%s](%s) - %.2f", _anchorText, getUrl(), _score);
    }

	public float getScore() {
		return _score;
	}
	
	public void setScore(float score) {
		_score = score;
	}
}
