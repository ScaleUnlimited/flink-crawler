package com.scaleunlimited.flinkcrawler.crawldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;

public class DrumMapTest {
	static final Logger LOGGER = LoggerFactory.getLogger(DrumMapTest.class);
	
	@Test
	public void testPayload() throws Exception {
		File dataDir = new File("target/test/testPayload/data");
		DrumMap dm = new DrumMap(1000, CrawlStateUrl.averageValueLength(), dataDir, new DefaultCrawlDBMerger());
		dm.open();
		
		byte[] value = new byte[2];
		value[0] = 1;
		for (int i = 500; i > 0; i--) {
			value[1] = (byte)(i % 97);
			dm.add(i, value, new LongPayload(i));
		}
		
		dm.close();
		assertEquals(500, dm.size());
		
		LongPayload payload = new LongPayload();
		for (int i = 1; i <= 500; i++) {
			assertTrue(dm.getInMemoryEntry(i, value, payload));
			assertEquals(1, value[0]);
			assertEquals((byte)(i % 97), value[1]);
			
			Long payloadValue = payload.getPayload();
			assertNotNull("Should get payload for key " + i, payloadValue);
			assertEquals(i, (long)payloadValue);
		}
	}

	@Ignore
	@Test
	public void testTiming() throws Exception {
		File dataDir = new File("target/test/testTiming/data");
		final int numEntries = 1_000_000;
		final IPayload payload = new LongPayload(0);
		
		for (int test = 0; test < 5; test++) {
			DrumMap dm = new DrumMap(numEntries, CrawlStateUrl.averageValueLength(), dataDir, new DefaultCrawlDBMerger());
			dm.open();
			Random rand = new Random(System.currentTimeMillis());

			long startTime = System.currentTimeMillis();
			long lastKey = 0;
			
			for (int i = 0; i < numEntries; i++) {
				// 1% of the entries are duplicates.
				if (((i + 1) % 100) == 0) {
					dm.add(lastKey, null, payload);
				} else {
					long key = rand.nextLong();
					dm.add(key, null, payload);
					lastKey = key;
				}
			}
			
			long deltaTime = System.currentTimeMillis() - startTime;
			LOGGER.info(String.format("Took %dms", deltaTime));
			dm.close();
		}
	}
	
	@Test
	public void testMerging() throws Exception {
		File dataDir = new File("target/test/testMerging/data");
		
		final int maxEntries = 1000;
		DrumMap dm = new DrumMap(maxEntries, CrawlStateUrl.averageValueLength(), dataDir, new DefaultCrawlDBMerger());
		dm.open();
		
		addUrl(dm, "http://domain.com/page0", FetchStatus.FETCHED, 10);
		addUrl(dm, "http://domain.com/page0", FetchStatus.FETCHED, 10);
		addUrl(dm, "http://domain.com/page1", FetchStatus.FETCHED, 1000);
		addUrl(dm, "http://domain.com/page1", FetchStatus.UNFETCHED, 0);
		addUrl(dm, "http://domain.com/page2", FetchStatus.UNFETCHED, 0);
		addUrl(dm, "http://domain.com/page3", FetchStatus.FETCHED, 100);

		FetchQueue queue = new FetchQueue(maxEntries);
		dm.merge(queue);
		
		// We should wind up with just one entry in the queue.
		FetchUrl urlToFetch = queue.poll();
		Assert.assertNotNull(urlToFetch);
		Assert.assertEquals("http://domain.com/page2", urlToFetch.getUrl());
		Assert.assertTrue(queue.isEmpty());
		
		// TODO verify what's in the active on-disk map.
		
		// Now, to ensure we're still usable after the merge, add a few more URLs and test again
		addUrl(dm, "http://domain.com/page4", FetchStatus.UNFETCHED, 100);
		addUrl(dm, "http://domain.com/page5", FetchStatus.UNFETCHED, 10000);
		dm.merge(queue);
		
		boolean gotPage4 = false;
		boolean gotPage5 = false;
		
		while (true) {
			urlToFetch = queue.poll();
			if (urlToFetch == null) {
				break;
			} else if (urlToFetch.getUrl().equals("http://domain.com/page4")) {
				Assert.assertFalse(gotPage4);
				gotPage4 = true;
			} else if (urlToFetch.getUrl().equals("http://domain.com/page5")) {
				Assert.assertFalse(gotPage5);
				gotPage5 = true;
			} else {
				Assert.fail("Unknown URL: " + urlToFetch.getUrl());
			}
		}
		
		Assert.assertTrue(gotPage4);
		Assert.assertTrue(gotPage5);
		
		// TODO verify if we have two unfetched for the same URL, we merge scores, take the lower
		// next fetch time, etc.
		
		dm.close();
	}
	
	@Test
	public void testMemoryDiskMerging() throws Exception {
		final String[] testCases = {
				"",			// nothing in list
				"0",		// single entry (unfetched)
				"1",		// single entry (fetched)
				"0,1",		// two different entries
				"1,2",
				"0,0",		// two entries that are the same
				"1,1",		// two entries that are the same
				"0,0,1",	// two dups, then new
				"0,0,2",	// two dups, then new
				"1,1,2",	// two dups, then new
				"1,1,3",	// two dups, then new
				"0,1,1",	// one different, then two dups.
				"0,2,2",	// one different, then two dups.
				"1,2,2",	// one different, then two dups.
				"1,3,3",	// one different, then two dups.
		};
		
		for (String memTestcase : testCases) {
			for (String diskTestcase : testCases) {
				File dataDir = new File("target/test/testMemoryDiskMerging/data");
				FileUtils.deleteDirectory(dataDir);
				createDiskFile(dataDir, diskTestcase);

				final int maxEntries = 1000;
				DrumMap dm = new DrumMap(maxEntries, CrawlStateUrl.averageValueLength(), dataDir, new DefaultCrawlDBMerger());
				dm.open();

				if (!memTestcase.isEmpty()) {
					for (String pageID : memTestcase.split(",")) {
						addUrl(dm, "http://domain.com/page" + pageID, getFetchStatusFromPageID(pageID), 10);
					}
				}
				
				FetchQueue queue = new FetchQueue(maxEntries);
				dm.merge(queue);

				// TODO make sure we wind up with what we expect
			}
			
		}
	}
	
	/**
	 * Create the on-disk DRUM files, located in dataDir.
	 * 
	 * @param dataDir
	 * @param testcase
	 * @throws IOException 
	 */
	private void createDiskFile(File dataDir, String testcase) throws IOException {
		File workingDir = new File(dataDir, DrumMap.WORKING_SUBDIR_NAME);
		FileUtils.forceMkdir(workingDir);
		
		DrumMapFile active = new DrumMapFile(workingDir, DrumMap.ACTIVE_FILE_PREFIX, false);
		DrumKeyValueFile dkvf = active.getKeyValueFile();
		DrumPayloadFile dpf = active.getPayloadFile();
		DrumDataOutput ddo = dpf.getDrumDataOutput();
		byte[] valueBuffer = new byte[1 + CrawlStateUrl.maxValueLength()];
		
		if (!testcase.isEmpty()) {
			DrumKeyValue dkv = new DrumKeyValue();
			for (String pageID : testcase.split(",")) {
				String urlAsString = "http://domain.com/page" + pageID;
				CrawlStateUrl url = makeUrl(urlAsString, getFetchStatusFromPageID(pageID), 10);
				
				dkv.setPayloadOffset(ddo.getBytesWritten());
				ddo.writeUTF(urlAsString);
				
				dkv.setKeyHash(url.makeKey());
				dkv.setValue(url.getValue(valueBuffer));
				
				dkvf.write(dkv);
			}
		}
		
		active.close();
	}

	private FetchStatus getFetchStatusFromPageID(String pageID) {
		boolean isEven = (Integer.parseInt(pageID) % 2) == 0;
		return isEven ? FetchStatus.UNFETCHED : FetchStatus.FETCHED;
	}

	private void addUrl(DrumMap dm, String urlAsString, FetchStatus status, long nextFetchTime) throws IOException {
		byte[] valueBuffer = new byte[1 + CrawlStateUrl.maxValueLength()];
		CrawlStateUrl url = new CrawlStateUrl(new FetchUrl(new ValidUrl(urlAsString)), status, nextFetchTime);
		Assert.assertFalse(dm.add(url.makeKey(), url.getValue(valueBuffer), url));
	}

	private static CrawlStateUrl makeUrl(String urlAsString, FetchStatus status, long nextFetchTime) throws MalformedURLException {
		CrawlStateUrl url = new CrawlStateUrl(new FetchUrl(new ValidUrl(urlAsString)), status, nextFetchTime);
		return url;
	}
	
	private static class LongPayload implements IPayload {

		private Long _payload;
		
		public LongPayload() {}
		
		public LongPayload(long payload) {
			_payload = payload;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(_payload);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			_payload = in.readLong();
		}
		
		@Override
		public void clear() {
			_payload = null;
		}
		
		public Long getPayload() {
			return _payload;
		}
	}

}
