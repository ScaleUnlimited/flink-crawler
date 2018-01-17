package com.scaleunlimited.flinkcrawler.focused;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;

/**
 * A FetchQueue that archives any URL with a low score.
 * 
 * FUTURE - when add() is called for the first N, calculate percentile bucket
 * boundaries and cache the URLs. Once we have confidence in the distribution,
 * archive any in the lowest quartile, add any in the highest quartile, and
 * do something interesting :) with the ones in the middle.
 *
 */
@SuppressWarnings("serial")
public class FocusedFetchQueue extends FetchQueue {

	private final float _minFetchScore;
	
	public FocusedFetchQueue(int maxQueueSize, float minFetchScore) {
		super(maxQueueSize);
		
		_minFetchScore = minFetchScore;
	}
	
	@Override
	public FetchQueueResult add(CrawlStateUrl url) {
		if (url.getScore() >= _minFetchScore) {
			return super.add(url);
		} else {
			return FetchQueueResult.ARCHIVE;
		}
	}

}
