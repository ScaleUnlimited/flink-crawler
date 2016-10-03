package com.scaleunlimited.flinkcrawler.utils;

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

public class DrumMapTest {

	@Test
	public void testNoSpill() throws Exception {
		DrumMap dm = new DrumMap(1000);
		
		for (int i = 500; i > 0; i--) {
			assertTrue(dm.add(i, new Integer(i), null));
		}
		
		assertEquals(500, dm.size());
		
		for (int i = 1; i <= 500; i++) {
			assertFalse("Adding with existing key returned true for key = " + i, dm.add(i, null, null));
		}
		
		
		for (int i = 1; i <= 500; i++) {
			Integer value = (Integer)dm.getInMemoryEntry(i, null);
			assertNotNull(value);
			assertEquals(i, (int)value);
			
			// TODO test with non-null payload (should be cleared out)
		}
		
		dm.close();
	}
	
	@Test
	public void testPayload() throws Exception {
		DrumMap dm = new DrumMap(1000);
		
		for (int i = 500; i > 0; i--) {
			assertTrue(dm.add(i, null, new LongPayload(i)));
		}
		
		assertEquals(500, dm.size());
		
		for (int i = 1; i <= 500; i++) {
			assertFalse("Adding with existing key returned true for key = " + i, dm.add(i, null, null));
		}
		
		LongPayload payload = new LongPayload();
		for (int i = 1; i <= 500; i++) {
			Integer value = (Integer)dm.getInMemoryEntry(i, payload);
			assertNull(value);
			
			Long payloadValue = payload.getPayload();
			assertNotNull(payloadValue);
			assertEquals(i, (long)payloadValue);
		}
		
		dm.close();
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
