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
	public MergeResult doMerge(CrawlStateUrl firstValue, CrawlStateUrl secondValue, CrawlStateUrl mergedValue) {
		FetchStatus firstStatus = firstValue.getStatus();
		FetchStatus secondStatus = secondValue.getStatus();
		
		if (firstStatus == FetchStatus.UNFETCHED) {
			if (secondStatus != FetchStatus.UNFETCHED) {
				return MergeResult.USE_SECOND;
			} else {
				// We need to merge the two unfetched entries, which means combining
				// their scores. We'll also want to update the status time to the
				// more recent.
                mergedValue.setFrom(firstValue);
			    
	            long firstStatusTime = firstValue.getStatusTime();
	            long secondStatusTime = secondValue.getStatusTime();
			    mergedValue.setStatusTime(Math.max(firstStatusTime, secondStatusTime));
			    
				float firstScore = firstValue.getScore();
				float secondScore = secondValue.getScore();
				mergedValue.setScore(firstScore + secondScore);
				
				long firstFetchTime = firstValue.getNextFetchTime();
				long secondFetchTime = secondValue.getNextFetchTime();
				mergedValue.setNextFetchTime(Math.min(firstFetchTime, secondFetchTime));

				return MergeResult.USE_MERGED;
			}
		} else if (secondStatus == FetchStatus.UNFETCHED) {
			// firstValue is not unfetched, secondValue is unfetched, so use first.
			return MergeResult.USE_FIRST;
		} else {
			// Neither first nor second value is unfetched, so use the more recent one.
			// TODO actual do more useful things with status?
			long firstTime = firstValue.getStatusTime();
			long secondTime = secondValue.getStatusTime();
			
			if (secondTime > firstTime) {
				return MergeResult.USE_SECOND;
			} else {
				return MergeResult.USE_FIRST;
			}
		}
	}

}
