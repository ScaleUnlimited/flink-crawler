package com.scaleunlimited.flinkcrawler.crawldb;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public class SimpleCrawlDB extends BaseCrawlDB {

	private Map<String, CrawlStateUrl> _crawlState;
	private ConcurrentLinkedQueue<FetchUrl> _urls;

	@Override
	public void open() {
		_urls = new ConcurrentLinkedQueue<>();
		_crawlState = new HashMap<>();
	}

	@Override
	public void close() {
	}

	@Override
	public void add(CrawlStateUrl url) {
		// We need to track the status of a URL.
		String key = url.getUrl();
	    CrawlStateUrl curState = _crawlState.get(key);
		if (curState == null) {
            System.out.format("Adding new URL %s to the crawlDB (%s)\n", key, _crawlState.values());
			_crawlState.put(key, url);
	        _urls.add(new FetchUrl(url.getUrl(), url.getPLD(), url.getEstimatedScore(), url.getActualScore()));
		} else {
		    // We have a collision. if the incoming status in UNFETCHED, then we know whatever is in the
		    // crawlDB "wins".
		    FetchStatus curStatus = curState.getStatus();
		    FetchStatus newStatus = url.getStatus();
		    if (newStatus == FetchStatus.UNFETCHED) {
                // Current always wins, all done.
                System.out.format("Ignoring new unfetched URL %s, already in crawlDB\n", url);
		    } else if (curStatus == FetchStatus.UNFETCHED) {
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

	@Override
	public FetchUrl get() {
		return _urls.poll();
	}

}
