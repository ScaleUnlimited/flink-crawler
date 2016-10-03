package com.scaleunlimited.flinkcrawler.utils;

/**
 * A fast sort routine for the arrays that comprise the in-memory data for a DRUM.
 * 	
 *	keys - an array of long hash values.
 *  offsets - an array of offsets into a payload file
 *  values - an array of (possibly null) objects that are associated with the key.
 *  
 * We sort using the key, and move entries in the other two arrays whenever we reorder
 * entries in the keys array.
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
    private static void swap(long[] keys, int i, int j, int[] offsets, Object[] values) {
        long temp = keys[i];
        keys[i] = keys[j];
        keys[j] = temp;

        int tempOffset = offsets[i];
        offsets[i] = offsets[j];
        offsets[j] = tempOffset;
        
        Object tempValue = values[i];
        values[i] = values[j];
        values[j] = tempValue;
    }

    /**
     * Internal insertion sort routine for subarrays of {@link long}s that is used by quicksort.
     * 
     * @param keys
     *            an array of long keys.
     * @param left
     *            the left-most index of the subarray.
     * @param right
     *            the right-most index of the subarray.
     * @param offsets
     *            The array of offsets to sort associative to the given keys array
     * @param values
     *            The array of values to sort associative to the given keys array
     */
    private static void insertionSort(long[] keys, int left, int right, int[] offsets, Object[] values) {
        for (int p = left + 1; p <= right; p++) {
            long tmp = keys[p];
            int tmpOffset = offsets[p];
            Object tmpValue = values[p];

            int j;
            for (j = p; j > left && (tmp < keys[j - 1]); j--) {
                keys[j] = keys[j - 1];
                offsets[j] = offsets[j - 1];
                values[j] = values[j - 1];
            }
            keys[j] = tmp;
            offsets[j] = tmpOffset;
            values[j] = tmpValue;
        }
    }

    /**
     * Quicksort for {@link longs}s. <br />
     * Algorithm of O( n*log(n) ) asymptotic upper bound.
     * 
     * @param keys
     *            The array to be sorted.
     * @param offsets
     *            The array of offsets to sort associative to the given keys array
     * @param values
     *            The array of values to sort associative to the given keys array
     * @return A reference to the array that was sorted.
     */
    public static long[] quickSort(long[] keys, int[] offsets, Object[] values) {
        return quickSort(keys, 0, keys.length - 1, offsets, values);
    }

    /**
     * Quicksort for longs. The bounds specify which part of the array is to be sorted.<br />
     * <br />
     * Algorithm of O( n*log(n) ) asymptotic upper bound. <br />
     * This version of quicksort also allows for bounds to be put in to specify what part of the array will be sorted.
     * <br />
     * The part of the array that lies between <b>left</b> and <b>right</b> is the only part that will be sorted.
     * 
     * @param keys
     *            The array to be sorted.
     * @param left
     *            The left boundary of what will be sorted.
     * @param right
     *            The right boundary of what will be sorted.
     * @param offsets
     *            The array of offsets to sort associative to the given keys array
     * @param values
     *            The array of values to sort associative to the given keys array
     * @return The keys array.
     */
    public static long[] quickSort(long[] keys, int left, int right, int[] offsets, Object[] values) {
        if (left + MIN_SIZE_FOR_QUICKSORT <= right) {
            int partitionIndex = partition(keys, left, right, offsets, values);
            quickSort(keys, left, partitionIndex - 1, offsets, values);
            quickSort(keys, partitionIndex, right, offsets, values);
        } else {
            // Do an insertion sort on the subarray
            insertionSort(keys, left, right, offsets, values);
        }
        
        return keys;
    }

    /**
     * Partitions part of an array of {@link long}s. <br />
     * The part of the array between <b>left</b> and <b>right</b> will be partitioned around the value held at
     * keys[right-1].
     * 
     * @param keys
     *            The array to be partitioned.
     * @param left
     *            The left bound of the array.
     * @param right
     *            The right bound of the array.
     * @param offsets
     *            The array of offsets to sort associative to the given keys array
     * @param values
     *            The array of values to sort associative to the given keys array
     * @return The index of the pivot after the partition has occurred.
     */
    private static int partition(long[] keys, int left, int right, int[] offsets, Object[] values) {
        int i = left;
        int j = right;
        long pivot = keys[(left + right) / 2];
        while (i <= j) {
            while (keys[i] < pivot)
                i++;
            while (keys[j] > pivot)
                j--;

            if (i <= j) {
                swap(keys, i, j, offsets, values);
                i++;
                j--;
            }
        }

        return i;
    }


}
