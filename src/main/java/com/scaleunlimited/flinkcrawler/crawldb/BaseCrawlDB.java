package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Serializable;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;

@SuppressWarnings("serial")
public abstract class BaseCrawlDB implements Serializable {

	protected FetchQueue _fetchQueue;
	
	/**
	 * Open the CrawlDB, and load entries into the fetch queue if we
	 * have ones available.
	 * 
	 * @param fetchQueue Queue used externally as source of URLs to be fetched
	 */
	public void open(FetchQueue fetchQueue) throws Exception {
		_fetchQueue = fetchQueue;
	}
	
	public abstract void close() throws Exception;
	
	/**
	 * Add a URL to the crawl DB.
	 * 
	 * WARNING - this call is asynchronous with respect to the get() call.
	 * 
	 * @param url URL to add.
	 */
	public abstract void add(CrawlStateUrl url) throws Exception;
	
	/**
	 * Reload the fetch queue with good entries.
	 */
	public abstract void merge() throws Exception;
}
