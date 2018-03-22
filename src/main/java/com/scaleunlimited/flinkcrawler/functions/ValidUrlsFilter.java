package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlValidator;

@SuppressWarnings("serial")
public class ValidUrlsFilter extends BaseFlatMapFunction<RawUrl, CrawlStateUrl> {
    static final Logger LOGGER = LoggerFactory.getLogger(ValidUrlsFilter.class);

    private BaseUrlValidator _urlValidator;

    public ValidUrlsFilter(BaseUrlValidator urlValidator) {
        _urlValidator = urlValidator;
    }

    @Override
    public void flatMap(RawUrl url, Collector<CrawlStateUrl> collector) throws Exception {
        record(this.getClass(), url);

        String urlAsString = url.getUrl();
        if (_urlValidator.isValid(urlAsString)) {
            try {
                ValidUrl validatedUrl = new ValidUrl(urlAsString);
                CrawlStateUrl crawlStateUrl = new CrawlStateUrl(validatedUrl, FetchStatus.UNFETCHED,
                        System.currentTimeMillis());
                crawlStateUrl.setScore(url.getScore());
                collector.collect(crawlStateUrl);
            } catch (MalformedURLException e) {
                LOGGER.warn("Filtering malformed URL (not caught by validator!!!) " + urlAsString);
            } catch (Throwable t) {
                LOGGER.error("Failure to collect: " + t.getMessage(), t);
            }
        } else {
            // Don't output anything, as we're filtering
            LOGGER.debug("Filtering invalid URL " + urlAsString);
        }
    }

}
