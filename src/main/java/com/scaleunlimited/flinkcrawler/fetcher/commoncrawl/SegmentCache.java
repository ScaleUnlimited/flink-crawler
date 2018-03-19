package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import java.util.LinkedHashMap;

/**
 * An LRU cache for segments, referenced by segment id.
 *
 */
public class SegmentCache {
    private static final int AVERAGE_SEGMENT_SIZE = 190_000;

    private LinkedHashMap<Integer, byte[]> _cache;

    @SuppressWarnings("serial")
    public SegmentCache(final int maxCacheSize) {
        int targetEntries = Math.max(1, maxCacheSize / AVERAGE_SEGMENT_SIZE);
        _cache = new LinkedHashMap<Integer, byte[]>(targetEntries, 0.75f, true) {

            @Override
            protected boolean removeEldestEntry(java.util.Map.Entry<Integer, byte[]> eldest) {
                return calcCacheSize() > maxCacheSize;
            }

            private int calcCacheSize() {
                int curSize = 0;
                for (byte[] data : values()) {
                    curSize += data.length;
                }

                return curSize;
            }
        };
    }

    public void put(int segmentId, byte[] data) {
        synchronized (_cache) {
            _cache.put(segmentId, data);
        }
    }

    public byte[] get(int segmentId) {
        synchronized (_cache) {
            return _cache.get(segmentId);
        }
    }
}
