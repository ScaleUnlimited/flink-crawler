package com.scaleunlimited.flinkcrawler.utils;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

public class ByteUtilsTest {

	@Test
	public void test() {
		final int numLongs = 1000;
		byte[] data = new byte[8 * numLongs];
		long[] values = new long[numLongs];
		
		Random rand = new Random(System.currentTimeMillis());
		
		int offset = 0;
		for (int i = 0; i < numLongs; i++) {
			long randValue = rand.nextLong();
			values[i] = randValue;
			ByteUtils.longToBytes(randValue, data, offset);
			offset += 8;
		}
		
		offset = 0;
		for (int i = 0; i < numLongs; i++) {
			assertEquals("Checking value #" + i, values[i], ByteUtils.bytesToLong(data, offset));
			offset += 8;
		}
		
	}

}
