package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import static org.junit.Assert.*;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class SegmentCacheTest {

    @Test
    public void testLRU() {
        final int entrySize = 10;
        SegmentCache cache = new SegmentCache(entrySize * 2);

        for (int i = 0; i < 100; i++) {
            cache.put(i, new byte[entrySize]);
            cache.get(0);
        }

        // Now, at the end of all this, we should have the cache entry
        // for segment 0, since we kept that one in the cache by always
        // getting it.
        assertNotNull(cache.get(0));
    }

    @Test
    public void testMultithreading() throws Exception {
        final int entrySize = 1;
        final SegmentCache cache = new SegmentCache(entrySize * 2);
        final Random rand = new Random(1L);
        final AtomicInteger numExceptions = new AtomicInteger();

        ThreadGroup group = new ThreadGroup("testMultithreading");
        for (int i = 0; i < 100; i++) {
            final int id = i;
            Thread t = new Thread(group, new Runnable() {

                @Override
                public void run() {
                    for (int j = 0; j < 10000; j++) {
                        if (rand.nextBoolean()) {
                            cache.put(j, new byte[entrySize]);
                        } else {
                            cache.get(j);
                        }
                    }
                }
            }, "thread-" + id);

            t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    numExceptions.incrementAndGet();
                }
            });

            t.start();
        }

        while (group.activeCount() > 0) {
            Thread.sleep(1L);
        }

        assertEquals(0, numExceptions.get());
    }

}
