package com.scaleunlimited.flinkcrawler.functions;

import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public class OutlinkToStateUrlFunction
        extends BaseMapFunction<ExtractedUrl, RawUrl> {

    @Override
    public RawUrl map(ExtractedUrl outlinkUrl) throws Exception {
        record(this.getClass(), outlinkUrl);
        return new RawUrl(outlinkUrl.getUrl(), outlinkUrl.getScore());
    }
}
