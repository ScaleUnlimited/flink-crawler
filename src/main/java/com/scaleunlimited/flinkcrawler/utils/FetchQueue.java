package com.scaleunlimited.flinkcrawler.utils;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

public class FetchQueue {
	
	private Queue<FetchUrl> _fetchQueue;
	private int _maxQueueSize;
	
	public FetchQueue(int maxQueueSize) {
		_maxQueueSize = maxQueueSize;
		_fetchQueue = new ConcurrentLinkedQueue<>();
	}
	
	public boolean isEmpty() {
		return _fetchQueue.isEmpty();
	}
	
	public boolean add(CrawlStateUrl url) {
		FetchStatus status = url.getStatus();
		
		// TODO make this more sophisticated.
		if ((_fetchQueue.size() < _maxQueueSize) && (status == FetchStatus.UNFETCHED)) {
			_fetchQueue.add(new FetchUrl(url, url.getEstimatedScore(), url.getActualScore()));
			return true;
		} else {
			return false;
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
