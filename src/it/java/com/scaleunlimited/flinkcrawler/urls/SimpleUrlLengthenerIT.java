package com.scaleunlimited.flinkcrawler.urls;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.FetchUtils;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

import crawlercommons.fetcher.http.UserAgent;

public class SimpleUrlLengthenerIT {

    private static final String TEST_SOURCE_URL = "http://bit.ly/2CCTt5C";
    private static final String TEST_DESTINATION_URL = "https://www.scaleunlimited.com/";
    private SimpleUrlLengthener _lengthener;
    private SimpleUrlNormalizer _normalizer = new SimpleUrlNormalizer();

    @Before
    public void setUp() throws Exception {
        UserAgent userAgent = new UserAgent("flink-crawler", "flink-crawler@scaleunlimited.com",
                "https://github.com/ScaleUnlimited/flink-crawler/wiki/Crawler-Policy");
        BaseHttpFetcherBuilder fetcherBuilder = FetchUtils.makeRedirectFetcherBuilder(1, userAgent);
        _lengthener = new SimpleUrlLengthener(fetcherBuilder);
        _lengthener.open();
    }

    @Test
    public void testLengthen() throws Throwable {
        RawUrl sourceUrl = new RawUrl(_normalizer.normalize(TEST_SOURCE_URL));
        RawUrl targetUrl = _lengthener.lengthen(sourceUrl);
        Assert.assertEquals(_normalizer.normalize(TEST_DESTINATION_URL), targetUrl.getUrl());
    }

}
