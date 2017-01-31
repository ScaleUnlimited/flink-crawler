package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlValidator;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings("serial")
public class ValidUrlsFilter extends RichFlatMapFunction<RawUrl, CrawlStateUrl> {
	static final Logger LOGGER = LoggerFactory.getLogger(ValidUrlsFilter.class);
	
	private BaseUrlValidator _urlValidator;
	
	public ValidUrlsFilter(BaseUrlValidator urlValidator) {
		_urlValidator = urlValidator;
	}

	@Override
	public void flatMap(RawUrl url, Collector<CrawlStateUrl> collector) throws Exception {
		UrlLogger.record(this.getClass(), url);
		
		String urlAsString = url.getUrl();
		if (_urlValidator.isValid(urlAsString)) {
			try {
				ValidUrl validatedUrl = new ValidUrl(urlAsString);
				collector.collect(new CrawlStateUrl(validatedUrl, FetchStatus.UNFETCHED, Float.NaN, url.getEstimatedScore(), 0, 0));
			} catch (MalformedURLException e) {
				LOGGER.debug("Filtering malformed URL " + urlAsString);
			}
		} else {
			// Don't output anything, as we're filtering
			LOGGER.debug("Filtering invalid URL " + urlAsString);
		}
	}

}
