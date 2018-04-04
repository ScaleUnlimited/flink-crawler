package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;

@SuppressWarnings({
        "serial"
})
public class HandleFailedSiteMapFunction extends RichFilterFunction<CrawlStateUrl> {
    static final Logger LOGGER = LoggerFactory.getLogger(HandleFailedSiteMapFunction.class);

    public HandleFailedSiteMapFunction() {
        super();
    }

    @Override
    public boolean filter(CrawlStateUrl crawlStateUrl) throws Exception {
        // only log if failed
        FetchStatus status = crawlStateUrl.getStatus();
        if (status != FetchStatus.FETCHED) {
            LOGGER.info("Failed fetching sitemap url '{}' due to '{}'",
                    crawlStateUrl.getUrl(), crawlStateUrl.getStatus());
        }

        return true;
    }
}
