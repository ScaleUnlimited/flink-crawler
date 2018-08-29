package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.java.functions.KeySelector;

import com.scaleunlimited.flinkcrawler.pojos.DomainScore;

@SuppressWarnings("serial")
public class DomainScoreKeySelector implements KeySelector<DomainScore, String> {

    @Override
    public String getKey(DomainScore value) throws Exception {
        return value.getPld();
    }

}
