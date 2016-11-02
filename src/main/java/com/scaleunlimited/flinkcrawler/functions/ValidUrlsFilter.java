package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RichFilterFunction;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlValidator;

@SuppressWarnings("serial")
public class ValidUrlsFilter extends RichFilterFunction<RawUrl> {

	private BaseUrlValidator _urlValidator;
	
	public ValidUrlsFilter(BaseUrlValidator urlValidator) {
		_urlValidator = urlValidator;
	}

	@Override
	public boolean filter(RawUrl input) throws Exception {
		String url = input.getUrl();
		boolean result = _urlValidator.isValid(url);
		if (result) {
			System.out.println("Not filtering " + url);
		} else {
			System.out.println("Filtering " + url);
		}
		
		return result;
	}

}
