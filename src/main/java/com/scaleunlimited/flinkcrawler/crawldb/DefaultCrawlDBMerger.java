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
		
		if (oldStatus == FetchStatus.UNFETCHED) {
			if (newStatus != FetchStatus.UNFETCHED) {
				return MergeResult.USE_NEW;
			}
		} else if (newStatus == FetchStatus.UNFETCHED) {
			// Old is not unfetched, new is unfetched, so use old.
			return MergeResult.USE_OLD;
		}
		
		// Both old/new status are unfetched, or both old/new are NOT
		// unfetched...in either case we want to use the more recent one.
		long oldTime = CrawlStateUrl.getFetchStatusTime(oldValue);
		long newTime = CrawlStateUrl.getFetchStatusTime(newValue);
		
		if (oldTime <= newTime) {
			return MergeResult.USE_NEW;
		} else {
			return MergeResult.USE_OLD;
		}

	}

}
