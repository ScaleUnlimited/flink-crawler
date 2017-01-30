package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlValidator;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings("serial")
public class ValidUrlsFilter extends RichFilterFunction<RawUrl> {
	static final Logger LOGGER = LoggerFactory.getLogger(ValidUrlsFilter.class);
	
	private BaseUrlValidator _urlValidator;
	
	public ValidUrlsFilter(BaseUrlValidator urlValidator) {
		_urlValidator = urlValidator;
	}

	@Override
	public boolean filter(RawUrl input) throws Exception {
		UrlLogger.record(this.getClass(), input);

		String url = input.getUrl();
		boolean result = _urlValidator.isValid(url);
		if (result) {
			LOGGER.debug("Not filtering " + url);
		} else {
			LOGGER.debug("Filtering " + url);
		}
		
		return result;
	}

}
