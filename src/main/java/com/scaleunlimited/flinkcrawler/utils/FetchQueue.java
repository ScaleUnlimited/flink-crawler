package com.scaleunlimited.flinkcrawler.utils;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

public class FetchQueue {
	
	public static enum UrlState {
		ACTIVE,
		ARCHIVE
	}

	private Queue<FetchUrl> _fetchQueue;
	private int _maxQueueSize;
	
	public FetchQueue(int maxQueueSize) {
		_maxQueueSize = maxQueueSize;
		_fetchQueue = new ConcurrentLinkedQueue<>();
	}
	
	public boolean isEmpty() {
		return _fetchQueue.isEmpty();
	}
	
	public UrlState add(CrawlStateUrl url) {
		FetchStatus fetchStatus = url.getStatus();
		if (fetchStatus == FetchStatus.UNFETCHED) {
			if (_fetchQueue.size() < _maxQueueSize) {
				url.setStatus(FetchStatus.FETCHING);
				_fetchQueue.add(new FetchUrl(url, url.getScore()));
			}
			
			return UrlState.ACTIVE;
		} else {
			// TODO decide based on (score? other factors) whether to keep around or not.
			return UrlState.ACTIVE;
		}
	}

	public FetchUrl poll() {
		return _fetchQueue.poll();
	}

	public boolean equals(Object o) {
		return _fetchQueue.equals(o);
	}

	public int hashCode() {
		return _fetchQueue.hashCode();
	}
	

}
