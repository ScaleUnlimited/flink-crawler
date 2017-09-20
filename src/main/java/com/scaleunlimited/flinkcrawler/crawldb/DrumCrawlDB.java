package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.File;

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
	private String _dataDirname;
	
	private transient DrumMap _drumMap;
	private transient byte[] _urlValue;
	
	public DrumCrawlDB(int maxRamEntries, String dataDirname) {
		_maxRamEntries = maxRamEntries;
		_dataDirname = dataDirname;
	}
	
	@Override
	public void open(int index, FetchQueue fetchQueue, BaseCrawlDBMerger merger) throws Exception {
		super.open(index, fetchQueue, merger);

		File topDataDir = new File(_dataDirname);
		File dataDir = new File(topDataDir, "index-" + index);
		_drumMap = new DrumMap(_maxRamEntries, CrawlStateUrl.VALUE_LENGTH, dataDir, merger);
		_drumMap.open();
		
		_urlValue = new byte[CrawlStateUrl.VALUE_SIZE];
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
	public boolean add(CrawlStateUrl url) throws Exception {
		synchronized (_drumMap) {
			url.getValue(_urlValue);
			return _drumMap.add(HashUtils.longHash(url.getUrl()), _urlValue, url);
		}
	}

	@Override
	public void merge() throws Exception {
		synchronized (_drumMap) {
			_drumMap.merge(_fetchQueue);
		}
	}

}
