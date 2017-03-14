package com.scaleunlimited.flinkcrawler.parser;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.config.ParserPolicy;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;

import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.sitemaps.SiteMapURL;

@SuppressWarnings("serial")
public class SimpleSiteMapParser extends BasePageParser {
    static final Logger LOGGER = LoggerFactory.getLogger(SimpleSiteMapParser.class);

	transient SiteMapParser _siteMapParser = null;
	
    public SimpleSiteMapParser() {
        this(new ParserPolicy());
    }

    public SimpleSiteMapParser(ParserPolicy policy) {
		super(policy);
	}

	private void init() {
		if (_siteMapParser == null) {
			_siteMapParser = new SiteMapParser();
		}
	}
	
	@Override
	public ParserResult parse(FetchedUrl fetchedUrl) throws Exception {
		
		init();
		
        if (LOGGER.isTraceEnabled()) {
        	LOGGER.trace(String.format("Parsing sitemap %s", fetchedUrl.getFetchedUrl()));
        }

		AbstractSiteMap parsedSiteMap = _siteMapParser.parseSiteMap(fetchedUrl.getContent(), new URL(fetchedUrl.getUrl()));
		
		if (parsedSiteMap instanceof SiteMap) {
			Collection<SiteMapURL> siteMapUrls = ((SiteMap) parsedSiteMap).getSiteMapUrls();
			ArrayList<ExtractedUrl> extractedUrls = new ArrayList<ExtractedUrl>();
			for (SiteMapURL siteMapURL : siteMapUrls) {
				extractedUrls.add(new ExtractedUrl(siteMapURL.getUrl().toExternalForm(), null, null));
			}
			return new ParserResult(null, extractedUrls.toArray(new ExtractedUrl[extractedUrls.size()]));
		} else {
			if (parsedSiteMap instanceof SiteMapIndex) {
				// Log this  - so we can deal with this in the future
				LOGGER.info("Unexpected SiteMapIndex encountered while parsing sitemap url: " + fetchedUrl.getFetchedUrl());
			} else {
				LOGGER.warn("Unknown type for AbstractSiteMap encountered: " + parsedSiteMap.getClass().getName());
			}
		}
		
		return null;
	}

}
