package com.scaleunlimited.flinkcrawler.pojos;

import static org.junit.Assert.*;

import org.junit.Test;

public class RawUrlTest {

    @Test
    public void testTicklerGeneration() {
        RawUrl url = RawUrl.makeRawTickerUrl(128, 2, 0);
        assertFalse(url.isRegular());
    }

}
