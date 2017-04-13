package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class BaseCrawlDBMerger implements Serializable {

	public static enum MergeResult {
		USE_OLD,
		USE_NEW,
		USE_MERGED
	}
	
	/**
	 * Merge <oldValue> with <newValue>, and put results into <mergedValue>
	 * 
	 * @param oldValue "value" from disk, first byte is length.
	 * @param newValue "value" from memory, first byte is length
	 * @param mergedValue Preallocated buffer for result, but only if USE_MERGED is returned
	 * @return Result of the merge.
	 */
	public abstract MergeResult doMerge(byte[] oldValue, byte[] newValue, byte[] mergedValue);
}
