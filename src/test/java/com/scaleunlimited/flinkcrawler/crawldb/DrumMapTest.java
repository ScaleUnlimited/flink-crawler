package com.scaleunlimited.flinkcrawler.crawldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;

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

	@Test
	public void testTiming() throws Exception {
		File dataDir = new File("target/test/testTiming/data");
		final int numEntries = 1_000_000;
		for (int test = 0; test < 5; test++) {
			DrumMap dm = new DrumMap(numEntries, CrawlStateUrl.averageValueLength(), dataDir, new DefaultCrawlDBMerger());
			dm.open();
			Random rand = new Random(System.currentTimeMillis());

			long startTime = System.currentTimeMillis();
			long lastKey = 0;
			
			for (int i = 0; i < numEntries; i++) {
				// 1% of the entries are duplicates.
				if (((i + 1) % 100) == 0) {
					dm.add(lastKey, null, null);
				} else {
					long key = rand.nextLong();
					dm.add(key, null, null);
					lastKey = key;
				}
			}
			
			long deltaTime = System.currentTimeMillis() - startTime;
			LOGGER.info(String.format("Took %dms", deltaTime));
			dm.close();
		}
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
