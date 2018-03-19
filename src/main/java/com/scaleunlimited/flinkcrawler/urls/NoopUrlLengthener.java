package com.scaleunlimited.flinkcrawler.urls;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

/**
 * Implementation of BaseUrlLengthener that does nothing. This lets us easily skip lengthening without modifying the
 * CrawlTopology.
 *
 */
@SuppressWarnings("serial")
public class NoopUrlLengthener extends BaseUrlLengthener {

    @Override
    public void open() throws Exception {
        // Do nothing
    }

    @Override
    public RawUrl lengthen(RawUrl url) {
        return url;
    }

    @Override
    public int getTimeoutInSeconds() {
        return 10;
    }

}
