package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;

@SuppressWarnings("serial")
public class PldKeySelector<T extends ValidUrl> implements KeySelector<T, String> {
    static final Logger LOGGER = LoggerFactory.getLogger(PldKeySelector.class);

    @Override
    public String getKey(T url) throws Exception {
//        if (LOGGER.isTraceEnabled()) {
//            LOGGER.trace("the key of {} is {}", url, url.getPld());
//        }
        return url.getPld();
    }
}
