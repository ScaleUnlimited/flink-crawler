package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Serializable;

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
	 * @param mergedValue Preallocated buffer for result, but only if USE_MERGED is returned
	 * @return Result of the merge.
	 */
	public abstract MergeResult doMerge(byte[] firstValue, byte[] secondValue, byte[] mergedValue);
}
