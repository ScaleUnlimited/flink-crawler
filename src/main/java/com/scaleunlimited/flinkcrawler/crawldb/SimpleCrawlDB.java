package com.scaleunlimited.flinkcrawler.crawldb;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import com.scaleunlimited.flinkcrawler.config.FetchPolicy;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public class SimpleCrawlDB extends BaseCrawlDB {

	private Map<String, CrawlStateUrl> _crawlState;

	@Override
	public void open(FetchPolicy fetchPolicy, Queue<FetchUrl> fetchQueue, int maxQueueSize) {
		super.open(fetchPolicy, fetchQueue, maxQueueSize);
		
		_crawlState = new HashMap<>();
	}

	@Override
	public void close() {
		// TODO need to force a merge, but without adding any urls to the active queue
	}

	@Override
	public void add(CrawlStateUrl url) {
		// We need to track the status of a URL.
		String key = url.getUrl();
		synchronized (_crawlState) {
			CrawlStateUrl curState = _crawlState.get(key);
			if (curState == null) {
				System.out.format("Adding new URL %s to the crawlDB (%s)\n", key, _crawlState.values());
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
					System.out.format("Ignoring new unfetched URL %s, already in crawlDB\n", url);
				} else if ((curStatus == FetchStatus.UNFETCHED) || (curStatus == FetchStatus.FETCHING)) {
					// We know new status isn't unfetched, so we always want to update
					System.out.format("Updating previously unfetched URL %s in crawlDB\n", url);
					_crawlState.put(key, url);
				} else {
					// we have "real" status for both current and new, so we have to merge
					System.out.format("Merging current status of %s in crawlDB with new status of %s for %s\n", curStatus, newStatus, url);
					// TODO make it so
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB#merge()
	 * 
	 * This can get called at the same time as the add, so we have to synchronize.
	 */
	@Override
	public void merge() {
		synchronized (_crawlState) {
			for (String url : _crawlState.keySet()) {
				CrawlStateUrl curState = _crawlState.get(url);
				if (isFetchable(curState)) {
					// TODO limit queue size.
					System.out.format("Adding URL from crawlDB to fetchable queue: %s\n", url);
					curState.setStatus(FetchStatus.FETCHING);
					_fetchQueue.add(new FetchUrl(url, curState.getPLD(), curState.getEstimatedScore(), curState.getActualScore()));
				}
			}
		}
	}

	
}
