package com.scaleunlimited.flinkcrawler.crawldb;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

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
		long[] keys = new long[numEntries];
		int[] offsets = new int[numEntries];
		Integer[] values = new Integer[numEntries];
		
		for (int i = 0; i < numEntries; i++) {
			keys[i] = numEntries - i;
			offsets[i] = numEntries - i;
			values[i] = new Integer(numEntries - i);
		}
		
		DrumMapSorter.quickSort(keys, offsets, values);
		for (int i = 0; i < numEntries; i++) {
			assertEquals(i + 1, keys[i]);
			assertEquals(i + 1, offsets[i]);
			assertEquals(i + 1, (int)values[i]);
		}
	}

	private void testDuplicateSequence(int numEntries) {
		final int dupEntries = numEntries / 2;
		
		long[] keys = new long[numEntries];
		int[] offsets = new int[numEntries];
		Integer[] values = new Integer[numEntries];
		
		for (int i = 0; i < numEntries; i++) {
			if (i < dupEntries) {
				keys[i] = 0;
				offsets[i] = 0;
				values[i] = new Integer(0);
			} else {
				keys[i] = numEntries - i;
				offsets[i] = numEntries - i;
				values[i] = new Integer(numEntries - i);
			}
		}
		
		DrumMapSorter.quickSort(keys, offsets, values);
		for (int i = 0; i < numEntries; i++) {
			int targetValue = i < dupEntries ? 0 : (i + 1) - dupEntries;
			assertEquals(targetValue, keys[i]);
			assertEquals(targetValue, offsets[i]);
			assertEquals(targetValue, (int)values[i]);
		}
	}

}
