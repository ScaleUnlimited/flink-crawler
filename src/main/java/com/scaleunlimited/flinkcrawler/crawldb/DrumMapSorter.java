package com.scaleunlimited.flinkcrawler.crawldb;

import com.scaleunlimited.flinkcrawler.utils.ByteUtils;

/**
 * A fast sort routine for the arrays that comprise the in-memory data for a DRUM.
 * 	
 *	keyOffsets - an array of int offsets into keyValues.
 *  keyValues - an array of bytes that store the actual long values to sort by.
 *  
 */
public class DrumMapSorter {

    /**
     * Constant defining at which array size quicksort or insertion sort is used.
     * 
     * This parameter was chosen by making performance tests for arrays of the sizes 50 - 500000 and different
     * parameters in the range 2 - 200.
     */
    public static int MIN_SIZE_FOR_QUICKSORT = 36;

    /**
     * Swaps the element at index i with element at index j
     * 
     * @param keys
     *            The array of keys to be sorted.
     * @param i
     *            index of first element
     * @param j
     *            index of second element
     * @param offsets
     *            The array of offsets to sort associative to the given keys array
     * @param values
     *            The array of values to sort associative to the given keys array
     */
    private static void swap(int[] keyOffsets, int i, int j) {
        int temp = keyOffsets[i];
        keyOffsets[i] = keyOffsets[j];
        keyOffsets[j] = temp;
    }

    /**
     * Internal insertion sort routine for sub-arrays of {@link long}s that is used by quicksort.
     * 
     * @param keyOffsets
     *            The array to be sorted. It contains offsets into keyValues.
     * @param left
     *            the left-most index of the subarray.
     * @param right
     *            the right-most index of the subarray.
     * @param keyValues
     *            The array of bytes holding the actual key values.
     */
    private static void insertionSort(int[] keyOffsets, int left, int right, byte[] keyValues) {
        for (int p = left + 1; p <= right; p++) {
            long tmp = getKeyValue(keyOffsets, p, keyValues);
            int tmpOffset = keyOffsets[p];
            
            int j;
            for (j = p; j > left && (tmp < getKeyValue(keyOffsets, j - 1, keyValues)); j--) {
            	keyOffsets[j] = keyOffsets[j - 1];
            }
            
            keyOffsets[j] = tmpOffset;
        }
    }

    /**
     * Quicksort for {@link longs}s. <br />
     * Algorithm of O( n*log(n) ) asymptotic upper bound.
     * 
     * @param keyOffsets
     *            The array to be sorted. It contains offsets into keyValues.
     * @param keyValues
     *            The array of bytes holding the actual key values.
     * @return A reference to the array that was sorted.
     */
    public static int[] quickSort(int[] keyOffsets, byte[] keyValues) {
        return quickSort(keyOffsets, 0, keyOffsets.length - 1, keyValues);
    }

    /**
     * Quicksort for longs. The bounds specify which part of the array is to be sorted.<br />
     * <br />
     * Algorithm of O( n*log(n) ) asymptotic upper bound. <br />
     * This version of quicksort also allows for bounds to be put in to specify what part of the array will be sorted.
     * <br />
     * The part of the array that lies between <b>left</b> and <b>right</b> is the only part that will be sorted.
     * 
     * @param keyOffsets
     *            The array to be sorted. It contains offsets into keyValues.
     * @param left
     *            The left boundary of what will be sorted.
     * @param right
     *            The right boundary of what will be sorted.
     * @param keyValues
     *            The array of bytes holding the actual key values.
     * @return The keyOffsets array.
     */
    public static int[] quickSort(int[] keyOffsets, int left, int right, byte[] keyValues) {
        if (left + MIN_SIZE_FOR_QUICKSORT <= right) {
            int partitionIndex = partition(keyOffsets, left, right, keyValues);
            quickSort(keyOffsets, left, partitionIndex - 1, keyValues);
            quickSort(keyOffsets, partitionIndex, right, keyValues);
        } else {
            // Do an insertion sort on the subarray
            insertionSort(keyOffsets, left, right, keyValues);
        }
        
        return keyOffsets;
    }

    /**
     * Partitions part of an array of {@link long}s. <br />
     * The part of the array between <b>left</b> and <b>right</b> will be partitioned around the value held at
     * keys[right-1].
     * 
     * @param keyOffsets
     *            The array to be partitioned. It contains offsets into keyValues.
     * @param left
     *            The left bound of the array.
     * @param right
     *            The right bound of the array.
     * @param keyValues
     *            The array of bytes holding the actual key values.
     * @return The index of the pivot after the partition has occurred.
     */
    private static int partition(int[] keyOffsets, int left, int right, byte[] keyValues) {
        int i = left;
        int j = right;
        long pivot = getKeyValue(keyOffsets, (left + right) / 2, keyValues);
        while (i <= j) {
            while (getKeyValue(keyOffsets, i, keyValues) < pivot)
                i++;
            while (getKeyValue(keyOffsets, j, keyValues) > pivot)
                j--;

            if (i <= j) {
                swap(keyOffsets, i, j);
                i++;
                j--;
            }
        }

        return i;
    }

	private static long getKeyValue(int[] keyOffsets, int i, byte[] keyValues) {
		return ByteUtils.bytesToLong(keyValues, keyOffsets[i]);
	}


}
