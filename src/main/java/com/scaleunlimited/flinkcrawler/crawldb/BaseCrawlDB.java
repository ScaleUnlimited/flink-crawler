package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Serializable;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public abstract class BaseCrawlDB implements Serializable {

	public abstract void open();
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
	 * Return the next URL to fetch, or null if no such URL is available.
	 * 
	 * WARNING - this call is asynchronous with respect to the add() call.
	 * 
	 * @return URL to fetch, or null
	 */
	public abstract FetchUrl get();
}
