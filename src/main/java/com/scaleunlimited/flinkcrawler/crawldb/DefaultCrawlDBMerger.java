package com.scaleunlimited.flinkcrawler.crawldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;


@SuppressWarnings("serial")
public class DefaultCrawlDBMerger extends BaseCrawlDBMerger {
    static final Logger LOGGER = LoggerFactory.getLogger(DefaultCrawlDBMerger.class);

	public DefaultCrawlDBMerger() {
		super();
	}
	
	@Override
	public MergeResult doMerge(byte[] oldValue, byte[] newValue, byte[] mergedValue) {
		FetchStatus oldStatus = CrawlStateUrl.getFetchStatus(oldValue);
		FetchStatus newStatus = CrawlStateUrl.getFetchStatus(newValue);
		
		if (newStatus == FetchStatus.UNFETCHED) {
			// We're getting a new URL that we already know about, so always use old.
			return MergeResult.USE_OLD;
		} else if (oldStatus == FetchStatus.FETCHING) {
			// new status must be an update from the fetch request.
			return MergeResult.USE_NEW;
		} else {
			// Old status must be coming off disk, and we've got something new
			// from a fetch request. We want to merge the two results.
			// TODO - make it so...for now, just use new.
			return MergeResult.USE_NEW;
		}
	}

}
