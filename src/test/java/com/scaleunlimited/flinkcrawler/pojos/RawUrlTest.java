package com.scaleunlimited.flinkcrawler.pojos;

import static org.junit.Assert.*;

import org.junit.Test;

public class RawUrlTest {

    @Test
    public void testContructors() {
        final String urlAsString = "http://domain.com/page";
        RawUrl url = new RawUrl(urlAsString, 2.0f);
        assertEquals(2.0f, url.getScore(), 0.001);
        
        RawUrl url2 = new RawUrl(url, 3.0f);
        assertEquals(urlAsString, url2.getUrl());
    }
}
