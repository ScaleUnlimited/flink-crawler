package com.scaleunlimited.flinkcrawler.crawldb;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;

@SuppressWarnings("serial")
public class InMemoryCrawlDB extends BaseCrawlDB {
    static final Logger LOGGER = LoggerFactory.getLogger(InMemoryCrawlDB.class);
    
	private Map<String, CrawlStateUrl> _crawlState;

	@Override
	public void open(FetchQueue fetchQueue) throws Exception {
		super.open(fetchQueue);
		
		_crawlState = new HashMap<>();
	}

	@Override
	public void close() throws Exception {
		// TODO need to force a merge, but without adding any urls to the active queue
	}

	@Override
	public boolean add(CrawlStateUrl url) throws Exception {
		// We need to track the status of a URL.
		String key = url.getUrl();
		synchronized (_crawlState) {
			CrawlStateUrl curState = _crawlState.get(key);
			if (curState == null) {
				LOGGER.debug(String.format("Adding new URL %s to the crawlDB (%s)", key, _crawlState.values()));
				// TODO here we'd want to check on size, and force a merge (in the background) if we're too big.
				// But this is testing only code, so don't worry about it.
				_crawlState.put(key, url);
			} else {
				// We have a collision. if the incoming status in UNFETCHED, then we know whatever is in the
				// crawlDB "wins".
				FetchStatus curStatus = curState.getStatus();
				FetchStatus newStatus = url.getStatus();
				if (newStatus == FetchStatus.UNFETCHED) {
					// Current always wins, all done.
					LOGGER.debug(String.format("Ignoring new unfetched URL %s, already in crawlDB", url));
				} else if ((curStatus == FetchStatus.UNFETCHED) || (curStatus == FetchStatus.FETCHING)) {
					// We know new status isn't unfetched, so we always want to update
					LOGGER.debug(String.format("Updating previously unfetched URL %s in crawlDB", url));
					_crawlState.put(key, url);
				} else {
					// we have "real" status for both current and new, so we have to merge
					LOGGER.debug(String.format("Merging current status of %s in crawlDB with new status of %s for %s", curStatus, newStatus, url));
					// TODO make it so
				}
			}
		}
		
		// We pretend like we'll never get full.
		// TODO have a limit to the size of _crawlState, and return true when it gets too big.
		return false;
	}

	/* (non-Javadoc)
	 * @see com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB#merge()
	 * 
	 * This can get called at the same time as the add, so we have to synchronize.
	 */
	@Override
	public void merge() throws Exception {
		synchronized (_crawlState) {
			for (String url : _crawlState.keySet()) {
				CrawlStateUrl curState = _crawlState.get(url);
				if (_fetchQueue.add(curState)) {
					LOGGER.debug(String.format("Added URL from crawlDB to fetchable queue: %s", curState));
					curState.setStatus(FetchStatus.FETCHING);
				}
			}
		}
	}

	
}
