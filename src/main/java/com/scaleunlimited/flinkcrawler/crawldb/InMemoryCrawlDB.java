package com.scaleunlimited.flinkcrawler.crawldb;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger.MergeResult;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue.MergeStatus;

@SuppressWarnings("serial")
public class InMemoryCrawlDB extends BaseCrawlDB {
    static final Logger LOGGER = LoggerFactory.getLogger(InMemoryCrawlDB.class);
    
	private transient Map<String, CrawlStateUrl> _crawlState;
	private transient Map<String, CrawlStateUrl> _archiveDB;

	private transient byte[] _curValue;
	private transient byte[] _newValue;
	private transient byte[] _mergedValue;
	
	public InMemoryCrawlDB() {
		super();
	}
	
	@Override
	public void open(int index, FetchQueue fetchQueue, BaseCrawlDBMerger merger) throws Exception {
		super.open(index, fetchQueue, merger);
		
		_crawlState = new HashMap<>();
		_archiveDB = new HashMap<>();
		
		_curValue = new byte[1 + CrawlStateUrl.maxValueLength()];
		_newValue = new byte[1  + CrawlStateUrl.maxValueLength()];
		_mergedValue = new byte[1 + CrawlStateUrl.maxValueLength()];
	}

	@Override
	public void close() throws Exception {
		// TODO need to force a merge, but without adding any urls to the active queue
	}

	@Override
	public boolean add(CrawlStateUrl url) throws Exception {
		String key = url.getUrl();
		synchronized (_crawlState) {
			CrawlStateUrl curState = _crawlState.get(key);
			if (curState == null) {
				LOGGER.debug(String.format("Adding new URL %s to the crawlDB", key));
				// TODO here we'd want to check on size, and force a merge (in the background) if we're too big.
				_crawlState.put(key, url);
			} else {
				curState.getValue(_curValue);
				url.getValue(_newValue);
				
				MergeResult result = _merger.doMerge(_curValue, _newValue, _mergedValue);
				if (result == MergeResult.USE_OLD) {
					// All set, nothing to do.
				} else if (result == MergeResult.USE_NEW) {
					curState.setFromValue(_newValue);
				} else if (result == MergeResult.USE_MERGED) {
					curState.setFromValue(_mergedValue);
				} else {
					throw new RuntimeException("Unknown merge result!");
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
				curState.getValue(_curValue);
				MergeStatus status = _fetchQueue.add(curState);
				
				if (status == MergeStatus.ACTIVE) {
					// Do nothing, just stays in the crawl DB
				} else if (status == MergeStatus.ARCHIVE) {
					// Remove from the in-memory DB, stick in our archive DB
					_crawlState.remove(url);
					_archiveDB.put(url, curState);
				} else {
					throw new RuntimeException("Unknown merge status: " + status);
				}
			}
		}
	}

}
