package com.scaleunlimited.flinkcrawler.crawldb;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.HashUtils;

/**
 * A crawl DB that uses the DRUM memory/disk approach for managing
 * the crawl frontier.
 *
 */
@SuppressWarnings("serial")
public class DrumCrawlDB extends BaseCrawlDB {

	private int _maxRamEntries;
	
	private transient DrumMap _drumMap;
	
	public DrumCrawlDB(int maxRamEntries) {
		_maxRamEntries = maxRamEntries;
	}
	
	@Override
	public void open(FetchQueue fetchQueue) throws Exception {
		super.open(fetchQueue);

		_drumMap = new DrumMap(_maxRamEntries);
		
		// Call merge to force anything on disk to be processed, so we
		// fill the fetch queue with whatever we've got.
		merge();
	}
	
	@Override
	public void close() throws Exception {
		_drumMap.close();
		// TODO how do we deal with URLs that are in the fetch queue? They have
		// their state set to "fetching". Or do we just handle that when we open?
		// But normally a merge wouldn't put anything that's being fetched back
		// into the fetch queue? So we might want to empty the fetch queue and
		// add them all back...or that feels like something which should be
		// handled by the function that's using us.
	}

	@Override
	public void add(CrawlStateUrl url) throws Exception {
		_drumMap.add(HashUtils.longHash(url.getUrl()), url.makeValue(), url);
	}

	@Override
	public void merge() throws Exception {
		// TODO Use as of yet 
	}

}
