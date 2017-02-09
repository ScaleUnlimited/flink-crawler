package com.scaleunlimited.flinkcrawler.crawldb;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.scaleunlimited.flinkcrawler.utils.ByteUtils;

public class DrumMapSorterTest {

	@Test
	public void testManyValues() {
		testLinearSequence(DrumMapSorter.MIN_SIZE_FOR_QUICKSORT * 100);
	}

	@Test
	public void testFewValues() {
		testLinearSequence(DrumMapSorter.MIN_SIZE_FOR_QUICKSORT - 1);
	}

	@Test
	public void testDuplicateValues() {
		testDuplicateSequence(DrumMapSorter.MIN_SIZE_FOR_QUICKSORT - 1);
		testDuplicateSequence(DrumMapSorter.MIN_SIZE_FOR_QUICKSORT * 100);
	}
	
	private void testLinearSequence(int numEntries) {
		int[] keyOffsets = new int[numEntries];
		byte[] keyData = new byte[numEntries * 8];
		
		int dataOffset = 0;
		for (int i = 0; i < numEntries; i++) {
			long key = numEntries - i;
			keyOffsets[i] = dataOffset;
			ByteUtils.longToBytes(key, keyData, dataOffset);
			dataOffset += 8;
		}
		
		DrumMapSorter.quickSort(keyOffsets, keyData);
		for (int i = 0; i < numEntries; i++) {
			assertEquals(i + 1L, ByteUtils.bytesToLong(keyData, keyOffsets[i]));
		}
	}

	private void testDuplicateSequence(int numEntries) {
		final int dupEntries = numEntries / 2;
		
		int[] keyOffsets = new int[numEntries];
		byte[] keyData = new byte[numEntries * 8];
		
		int dataOffset = 0;
		for (int i = 0; i < numEntries; i++) {
			long key = (i < dupEntries) ? 0 : numEntries - i;
			keyOffsets[i] = dataOffset;
			ByteUtils.longToBytes(key, keyData, dataOffset);
			dataOffset += 8;
		}
		
		DrumMapSorter.quickSort(keyOffsets, keyData);
		for (int i = 0; i < numEntries; i++) {
			long targetValue = i < dupEntries ? 0 : (i + 1) - dupEntries;
			
			assertEquals(targetValue, ByteUtils.bytesToLong(keyData, keyOffsets[i]));
		}
	}

}
