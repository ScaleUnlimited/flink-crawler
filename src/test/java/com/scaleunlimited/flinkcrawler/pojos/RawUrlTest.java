package com.scaleunlimited.flinkcrawler.pojos;

import static org.junit.Assert.*;

import org.junit.Test;

public class RawUrlTest {

    @Test
    public void testTicklerGeneration() {
        final int maxParallelism = 128;
        final int parallelism = 3;
        
        for (int operatorIndex = 0; operatorIndex < parallelism; operatorIndex++) {
            RawUrl url = RawUrl.makeRawTickerUrl(maxParallelism, parallelism, operatorIndex);
            System.out.format("%d: %s\n", operatorIndex, url.getUrl());
            assertFalse(url.isRegular());
        }
    }
}
