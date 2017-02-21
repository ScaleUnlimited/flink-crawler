package com.scaleunlimited.flinkcrawler.functions;

import java.net.URL;
import java.util.Collection;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.sitemaps.SiteMapURL;

@SuppressWarnings("serial")
public class ParseSiteMapFunction extends RichFlatMapFunction<FetchedUrl, Tuple2<ExtractedUrl, ParsedUrl>> {

    static final Logger LOGGER = LoggerFactory.getLogger(ParseSiteMapFunction.class);

	private SiteMapParser _siteMapParser;

	public ParseSiteMapFunction(SiteMapParser siteMapParser) {
		_siteMapParser = siteMapParser;
    }

	@Override
	public void flatMap(FetchedUrl fetchedUrl, Collector<Tuple2<ExtractedUrl, ParsedUrl>> collector) throws Exception {
			
		UrlLogger.record(this.getClass(), fetchedUrl);
		
		AbstractSiteMap parsedSiteMap = _siteMapParser.parseSiteMap(fetchedUrl.getContent(), new URL(fetchedUrl.getUrl()));
		
		if (parsedSiteMap instanceof SiteMap) {
			Collection<SiteMapURL> siteMapUrls = ((SiteMap) parsedSiteMap).getSiteMapUrls();
			// Output the links
			for (SiteMapURL siteMapURL : siteMapUrls) {
				ExtractedUrl extractedUrl = new ExtractedUrl(siteMapURL.getUrl().toExternalForm(), null, null);
				collector.collect(new Tuple2<ExtractedUrl, ParsedUrl>(extractedUrl, null));
			}
			
		} else {
			if (parsedSiteMap instanceof SiteMapIndex) {
				// Log this  - so we can deal with this in the future
				LOGGER.info("Unexpected SiteMapIndex encountered while parsing sitemap url: " + fetchedUrl.getFetchedUrl());
			} else {
				LOGGER.warn("Unknown type for AbstractSiteMap encountered: " + parsedSiteMap.getClass().getName());
			}
		}
	}

}
