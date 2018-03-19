package com.scaleunlimited.flinkcrawler.pojos;

import java.util.Map;

@SuppressWarnings("serial")
public class ParsedUrl extends ValidUrl {

    private String _parsedText;
    private String _language;
    private String _title;
    private float _score;
    private Map<String, String> _parsedMeta;

    public ParsedUrl() {
        super();
    }

    public ParsedUrl(ValidUrl url, String parsedText, String language, String title,
            Map<String, String> parsedMeta, float score) {
        super(url);

        _parsedText = parsedText;
        _language = language;
        _title = title;
        _parsedMeta = parsedMeta;
        _score = score;
    }

    public String getParsedText() {
        return _parsedText;
    }

    public void setParsedText(String parsedText) {
        _parsedText = parsedText;
    }

    public String getLanguage() {
        return _language;
    }

    public void setLanguage(String language) {
        _language = language;
    }

    public String getTitle() {
        return _title;
    }

    public void setTitle(String title) {
        _title = title;
    }

    public Map<String, String> getParsedMeta() {
        return _parsedMeta;
    }

    public void setParsedMeta(Map<String, String> parsedMeta) {
        _parsedMeta = parsedMeta;
    }

    public float getScore() {
        return _score;
    }

    public void setScore(float score) {
        _score = score;
    }

}
