package com.scaleunlimited.flinkcrawler.functions;

import static org.junit.Assert.*;

import org.junit.Test;

import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction.TimedCounter;

public class FetchUrlsFunctionTest {

    @Test
    public void testTimedCounter() {
        long curTime = System.currentTimeMillis();
        TimedCounter counter = new TimedCounter(1);
        assertEquals(0, counter.getTotalCounts());

        counter.increment(curTime);
        assertArrayEquals(new int[] {
                1
        }, counter.getCountsPerSecond());
        assertEquals(1, counter.getTotalCounts());

        counter.increment(curTime);
        assertArrayEquals(new int[] {
                2
        }, counter.getCountsPerSecond());

        curTime += 1000L;
        counter.increment(curTime);
        assertArrayEquals(new int[] {
                1
        }, counter.getCountsPerSecond());
        counter.increment(curTime);
        assertArrayEquals(new int[] {
                2
        }, counter.getCountsPerSecond());

        curTime += 10000L;
        counter.increment(curTime);
        assertArrayEquals(new int[] {
                1
        }, counter.getCountsPerSecond());

        // Now for the harder case, of say 3 seconds, with different
        // amounts of shifting.
        curTime = System.currentTimeMillis();
        counter = new TimedCounter(3);

        counter.increment(curTime);
        assertArrayEquals(new int[] {
                0, 0, 1
        }, counter.getCountsPerSecond());
        counter.increment(curTime);
        assertArrayEquals(new int[] {
                0, 0, 2
        }, counter.getCountsPerSecond());

        curTime += 1000;
        counter.increment(curTime);
        assertArrayEquals(new int[] {
                0, 2, 1
        }, counter.getCountsPerSecond());
        counter.increment(curTime);
        counter.increment(curTime);
        assertArrayEquals(new int[] {
                0, 2, 3
        }, counter.getCountsPerSecond());
        assertEquals(5, counter.getTotalCounts());

        curTime += 1000;
        counter.increment(curTime);
        assertArrayEquals(new int[] {
                2, 3, 1
        }, counter.getCountsPerSecond());

        curTime += 2000;
        counter.increment(curTime);
        assertArrayEquals(new int[] {
                1, 0, 1
        }, counter.getCountsPerSecond());

        curTime += 3000;
        counter.increment(curTime);
        assertArrayEquals(new int[] {
                0, 0, 1
        }, counter.getCountsPerSecond());
    }

}
