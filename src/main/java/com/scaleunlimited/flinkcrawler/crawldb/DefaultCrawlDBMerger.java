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
	public MergeResult doMerge(byte[] firstValue, byte[] secondValue, byte[] mergedValue) {
		FetchStatus firstStatus = CrawlStateUrl.getFetchStatus(firstValue);
		FetchStatus secondStatus = CrawlStateUrl.getFetchStatus(secondValue);
		
		if (firstStatus == FetchStatus.UNFETCHED) {
			if (secondStatus != FetchStatus.UNFETCHED) {
				return MergeResult.USE_SECOND;
			} else {
				// We need to merge the two unfetched entries, which means combining
				// their scores. We'll also want to update the status time to the
				// more recent.
				long firstStatusTime = CrawlStateUrl.getStatusTime(firstValue);
				long secondStatusTime = CrawlStateUrl.getStatusTime(secondValue);
				float firstScore = CrawlStateUrl.getScore(firstValue);
				float secondScore = CrawlStateUrl.getScore(secondValue);
				long firstFetchTime = CrawlStateUrl.getFetchTime(firstValue);
				long secondFetchTime = CrawlStateUrl.getFetchTime(secondValue);

				CrawlStateUrl.setValue(	mergedValue,
										FetchStatus.UNFETCHED, 
										Math.max(firstStatusTime, secondStatusTime), 
										firstScore + secondScore, 
										Math.min(firstFetchTime, secondFetchTime));
				return MergeResult.USE_MERGED;
			}
		} else if (secondStatus == FetchStatus.UNFETCHED) {
			// firstValue is not unfetched, secondValue is unfetched, so use old.
			return MergeResult.USE_FIRST;
		} else {
			// Neither first nor second value is unfetched, so use the more recent one.
			// TODO actual do more useful things with status?
			long firstTime = CrawlStateUrl.getStatusTime(firstValue);
			long secondTime = CrawlStateUrl.getStatusTime(secondValue);
			
			if (secondTime > firstTime) {
				return MergeResult.USE_SECOND;
			} else {
				return MergeResult.USE_FIRST;
			}
		}
	}

}
