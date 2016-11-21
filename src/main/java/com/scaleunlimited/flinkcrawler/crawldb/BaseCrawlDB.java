package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Serializable;
import java.util.Queue;

import com.scaleunlimited.flinkcrawler.config.FetchPolicy;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public abstract class BaseCrawlDB implements Serializable {

	protected Queue<FetchUrl> _fetchQueue;
	protected int _maxQueueSize;
	protected FetchPolicy _fetchPolicy;
	
	/**
	 * Open the CrawlDB, and load entries into the fetch queue if we
	 * have ones available.
	 * 
	 * @param fetchQueue
	 * @param maxQueueSize Max entries in fetchQueue
	 */
	public void open(FetchPolicy fetchPolicy, Queue<FetchUrl> fetchQueue, int maxQueueSize) {
		_fetchPolicy = fetchPolicy;
		_fetchQueue = fetchQueue;
		_maxQueueSize = maxQueueSize;
	}
	
	protected boolean isFetchable(CrawlStateUrl url) {
		return _fetchPolicy.isFetchable(url);
	}
	
	public abstract void close();
	
	/**
	 * Add a URL to the crawl DB.
	 * 
	 * WARNING - this call is asynchronous with respect to the get() call.
	 * 
	 * @param url URL to add.
	 */
	public abstract void add(CrawlStateUrl url);
	
	/**
	 * Merge the in-memory queue with the disk-based data (if any exists), and reload
	 * the fetch queue with good entries.
	 */
	public abstract void merge();
}
