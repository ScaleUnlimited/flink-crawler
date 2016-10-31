package com.scaleunlimited.flinkcrawler.crawldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.junit.Test;

public class DrumMapTest {

	@Test
	public void testPayload() throws Exception {
		DrumMap dm = new DrumMap(1000);
		
		for (int i = 500; i > 0; i--) {
			dm.add(i, null, new LongPayload(i));
		}
		
		dm.close();
		assertEquals(500, dm.size());
		
		LongPayload payload = new LongPayload();
		for (int i = 1; i <= 500; i++) {
			Integer value = (Integer)dm.getInMemoryEntry(i, payload);
			assertNull("Shouldn't find value for key " + i, value);
			
			Long payloadValue = payload.getPayload();
			assertNotNull("Should get payload for key " + i, payloadValue);
			assertEquals(i, (long)payloadValue);
		}
	}

	@Test
	public void testTiming() throws Exception {
		final int numEntries = 1_000_000;
		for (int test = 0; test < 10; test++) {
			DrumMap dm = new DrumMap(numEntries);
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
			System.out.format("Took %dms\n", deltaTime);
			dm.close();
		}
	}
	
	private static class LongPayload extends Payload {

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
