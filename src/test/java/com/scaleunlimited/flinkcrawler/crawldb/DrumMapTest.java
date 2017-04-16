package com.scaleunlimited.flinkcrawler.crawldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
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
		final String[][] testCases = {
		//	mem, disk, queue, active, archive
				{"1u,2u,2f,3u",	"1f,3u,4f", "3u", "1f,2f,3u,4f",	""},		// mix of entries

				{"",	"",		"", 	"", 	""},
		
			{"1u",	"",		"1u",	"1u",	""},		// single entry (unfetched)
			{"1f",	"",		"",		"1f",	""},		// single entry (fetched)
			{"",	"1u",	"1u",	"1u",	""},		// single entry (fetched)
			{"",	"1f",	"",		"1f",	""},		// single entry (fetched)
			
			{"1u,1u",	"",	"1u",	"1u",	""},		// dup entries (unfetched)
			{"1f,1f",	"",	"",		"1f",	""},		// dup entries (fetched)
			{"1u,1f",	"",	"",		"1f",	""},		// dup entries (unfetched & fetched)
			{"1f,1u",	"",	"",		"1f",	""},		// dup entries (fetched & unfetched)

			// Entries in both memory and disk
			{"1u",	"1u",	"1u",	"1u",	""},		// same entry (unfetched)
			{"1f",	"1f",	"",		"1f",	""},		// same entry (fetched)
			{"1u",	"1f",	"",		"1f",	""},		// same entry (unfetched & fetched)

			{"1u,2u,2f,3u",	"1f,3u,4f", "3u", "1f,2f,3u,4f",	""},		// mix of entries
			
			// Nothing in memory, all on disk.
			{"", "1f,2u", "2u", "1f,2u", ""},
		};
		
		for (String[] testCase : testCases) {
			String memTestCase = testCase[0];
			String diskTestCase = testCase[1];

			File dataDir = new File("target/test/testMemoryDiskMerging/data");
			FileUtils.deleteDirectory(dataDir);
			createDiskFile(dataDir, diskTestCase);

			final int maxEntries = 1000;
			DrumMap dm = new DrumMap(maxEntries, CrawlStateUrl.averageValueLength(), dataDir, new DefaultCrawlDBMerger());
			dm.open();

			if (!memTestCase.isEmpty()) {
				for (String pageID : memTestCase.split(",")) {
					addUrl(dm, getUrlFromPageID(pageID), getFetchStatusFromPageID(pageID), 10);
				}
			}

			FetchQueue queue = new FetchQueue(maxEntries);
			dm.merge(queue);

			String queueResults = testCase[2];
			if (queueResults.isEmpty()) {
				assertTrue("Fetch queue should be empty for test " + testCaseToString(testCase), queue.isEmpty());
			} else {
				for (String pageID : queueResults.split(",")) {
					assertFalse(queue.isEmpty());
					FetchUrl urlFromQueue = queue.poll();
					assertEquals(getUrlFromPageID(pageID), urlFromQueue.getUrl());
				}
			}
			
			Iterator<CrawlStateUrl> activeIter = dm.getActiveFile().iterator();
//			while (activeIter.hasNext()) {
//				System.out.println(activeIter.next());
//			}
			
			String activeResults = testCase[3];
			if (activeResults.isEmpty()) {
				assertFalse(activeIter.hasNext());
			} else {
				for (CrawlStateUrl expectedUrl : makeSortedResults(activeResults)) {
					assertTrue(activeIter.hasNext());
					CrawlStateUrl actualUrl = activeIter.next();
					assertEquals("Check URL for test " + testCaseToString(testCase), expectedUrl.getUrl(), actualUrl.getUrl());
					assertEquals("Check fetch status for test " + testCaseToString(testCase), expectedUrl.getStatus(), actualUrl.getStatus());
				}
			}
			
			Iterator<CrawlStateUrl> archivedIter = dm.getArchivedFile().iterator();
			String archiveResults = testCase[4];
			if (archiveResults.isEmpty()) {
				assertFalse(archivedIter.hasNext());
			} else {
				for (CrawlStateUrl expectedUrl : makeSortedResults(archiveResults)) {
					assertTrue(archivedIter.hasNext());
					CrawlStateUrl actualUrl = archivedIter.next();
					assertEquals("Check URL for test " + testCaseToString(testCase), expectedUrl.getUrl(), actualUrl.getUrl());
					assertEquals("Check fetch status for test " + testCaseToString(testCase), expectedUrl.getStatus(), actualUrl.getStatus());
				}
			}
		}
	}
	
	/**
	 * Results from active & archived files are in key hash order, so we need to do the same for
	 * our tests.
	 * 
	 * @param iterator
	 * @return
	 * @throws MalformedURLException 
	 */
	private List<CrawlStateUrl> makeSortedResults(String results) throws MalformedURLException {
		List<CrawlStateUrl> result = new ArrayList<>();
		for (String pageID : results.split(",")) {
			CrawlStateUrl url = makeUrl(getUrlFromPageID(pageID), getFetchStatusFromPageID(pageID), 0);
			result.add(url);
		}
		
		Collections.sort(result, new Comparator<CrawlStateUrl>() {

			@Override
			public int compare(CrawlStateUrl o1, CrawlStateUrl o2) {
				if (o1.makeKey() < o2.makeKey()) {
					return -1;
				} else if (o1.makeKey() > o2.makeKey()) {
					return 1;
				} else {
					return 0;
				}
			}
		});
		
		return result;
	}

	private String testCaseToString(String[] testCase) {
		StringBuilder result = new StringBuilder();
		result.append("mem: \"");
		result.append(testCase[0]);
		result.append("\", ");

		result.append("disk: \"");
		result.append(testCase[1]);
		result.append("\", ");

		result.append("queue: \"");
		result.append(testCase[2]);
		result.append("\", ");

		result.append("active: \"");
		result.append(testCase[3]);
		result.append("\", ");

		result.append("archive: \"");
		result.append(testCase[4]);
		result.append("\"");

		return result.toString();
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
		
		DrumMapFile active = new DrumMapFile(workingDir, DrumMap.ACTIVE_FILE_PREFIX, true);
		DrumKeyValueFile dkvf = active.getKeyValueFile();
		DrumPayloadFile dpf = active.getPayloadFile();
		DrumDataOutput ddo = dpf.getDrumDataOutput();
		byte[] valueBuffer = new byte[1 + CrawlStateUrl.maxValueLength()];
		
		if (!testcase.isEmpty()) {
			DrumKeyValue dkv = new DrumKeyValue();
			for (CrawlStateUrl url : makeSortedResults(testcase)) {
				
				dkv.setPayloadOffset(ddo.getBytesWritten());
				url.write(ddo);
				
				dkv.setKeyHash(url.makeKey());
				dkv.setValue(url.getValue(valueBuffer));
				
				dkvf.write(dkv);
			}
		}
		
		active.close();
	}

	private String getUrlFromPageID(String pageID) {
		String pageNumber = pageID.substring(0, 1);
		return "http://domain.com/page" + pageNumber;
	}

	private FetchStatus getFetchStatusFromPageID(String pageID) {
		String fetchStatus = pageID.substring(1, 2);
		if (fetchStatus.equals("u")) {
			return FetchStatus.UNFETCHED;
		} else if (fetchStatus.equals("f")) {
			return FetchStatus.FETCHED;
		} else {
			throw new RuntimeException("Unknown fetch status for test: " + fetchStatus);
		}
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
