package com.scaleunlimited.flinkcrawler.pojos;

import java.io.Serializable;

@SuppressWarnings("serial")
public class DomainScore implements Serializable {

    private String _pld;
    private float _score;
    
    public DomainScore(String pld, float score) {
        _pld = pld;
        _score = score;
    }

    public String getPld() {
        return _pld;
    }

    public void setPld(String pld) {
        _pld = pld;
    }

    public float getScore() {
        return _score;
    }

    public void setScore(float score) {
        _score = score;
    }
    
}
