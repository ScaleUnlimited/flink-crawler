package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Serializable;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;

@SuppressWarnings("serial")
public abstract class BaseCrawlDBMerger implements Serializable {

	public static enum MergeResult {
		USE_FIRST,
		USE_SECOND,
		USE_MERGED
	}
	
	/**
	 * Merge two entries, and put results into <mergedValue> if the results
	 * actually need to be merged.
	 * 
	 * @param firstValue
	 * @param secondValue
	 * @param mergedValue Preallocated object for result, but only if USE_MERGED is returned
	 * @return Result of the merge.
	 */
	public abstract MergeResult doMerge(CrawlStateUrl firstValue, CrawlStateUrl secondValue, CrawlStateUrl mergedValue);
}
