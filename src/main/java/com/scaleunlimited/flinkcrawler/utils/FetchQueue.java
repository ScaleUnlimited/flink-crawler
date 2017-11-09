package com.scaleunlimited.flinkcrawler.utils;

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public class FetchQueue implements Serializable {
	
	public static enum UrlState {
		ACTIVE,
		ARCHIVE
	}

	private int _maxQueueSize;
	
	private transient Queue<FetchUrl> _fetchQueue;
	
	public FetchQueue(int maxQueueSize) {
		_maxQueueSize = maxQueueSize;
	}
	
	/**
	 * Lifecycle management - called once we're deployed.
	 */
	public void open() {
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

	public int size() {
		return _fetchQueue.size();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_fetchQueue == null) ? 0 : _fetchQueue.hashCode());
		result = prime * result + _maxQueueSize;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FetchQueue other = (FetchQueue) obj;
		if (_fetchQueue == null) {
			if (other._fetchQueue != null)
				return false;
		} else if (!_fetchQueue.equals(other._fetchQueue))
			return false;
		if (_maxQueueSize != other._maxQueueSize)
			return false;
		return true;
	}


}
