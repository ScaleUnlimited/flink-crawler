package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.scaleunlimited.flinkcrawler.utils.BloomFilter;

/**
 * A DrumMap implements the DRUM storage system as described by the IRLBot paper
 * (see http://irl.cs.tamu.edu/people/hsin-tsang/papers/www2008.pdf)
 * 
 * There's an in-memory array of new key/value pairs, where the key is an 8-byte
 * (long) hash, the value is an (optional) Object of any time, and there's an
 * additional offset into a separate "payload" file, where associated data is kept.
 * 
 * We don't bother deduping keys as they're added. When the array is full, we'll
 * sort, and then (TODO) dedup if there are enough duplicate entries, otherwise
 * it's time for a merge.
 * 
 * During deduplication, a provided DrumEntryMerger class is called to merge the
 * old and new entries. The results might be to ignore the new entry,
 * replace the old entry, or update the old entry with the value and/or the payload
 * from the new entry.
 * 
 * When the in-memory array of entries is full, or there's an external trigger, then
 * a merge happens with an on-disk version of the in-memory map, and a separate/associated
 * payload file. During this merge the DrumMap is still usable, as a new in-memory array
 * is allocated, along with a new payload file.
 * 
 * The saved key/value and payload data is sorted by hash as well, so the merge operation
 * allocates a small buffer that is used to read in chunks of the key/value pairs, which
 * are then merged (using the same logic as when adding an existing entry to the in-memory
 * array). The merged results are written to a new key/value file, while new/merged
 * payloads are written to the end of the existing payload file.
 * 
 * During the merge, a DrumMapSplitter is called to decide on what to do with merged
 * entries. The three possibilities are "active" (as described above), "fetch" (which
 * writes to active and also puts the entry in a fetch queue), and "archive" (which
 * write to archives key/value and payload files).
 * 
 * TODO - decide about whether to keep the 'value' as a fixed number of bytes (e.g. 32)
 * We could have an int offset (or index) to the value in the actual map, which is-1 if no value.
 * So that way we avoid lots of objects being allocated. But all state required for
 * merging CrawlDBUrl (doesn't exist yet, but it should) would have to fit in that fixed
 * size. Merging code would treat the 'no value' case as what everything should be for
 * a new outlink with a score below some threshold, as that's what most URLs will be.
 * Hmm, we could have variable size values (e.g. nothing, vs just a score) so that every
 * new URL could have a score, but if there's no other state then we avoid storing that data.
 * 
 */
public class DrumMap implements Closeable {
	private static final int NO_PAYLOAD_OFFSET = -1;

	private static final int NO_VALUE_INDEX = -1;
	
	public static int DEFAULT_MAX_ENTRIES = 10_000;

	// Max number of entries in the array
	private int _maxEntries;
	
	// Number of entries in the in-memory arrays.
	private int _numEntries;
	
	// An in-memory array of keys (hashes), which are longs
	private long[] _keys;
	
	// An in-memory array of values, which are generic Objects
	private Object[] _values;
	
	// An in-memory array of offsets into the payload file
	private int[] _offsets;
	
	private DrumDataOutput _payloadOut;
	
	private BloomFilter _dupTracker;
	private int _numEstimatedDups;
	
	public DrumMap(int maxEntries) throws FileNotFoundException, IOException {
		_maxEntries = maxEntries;
		_numEntries = 0;
		
		_keys = new long[_maxEntries];
		_values = new Object[_maxEntries];
		_offsets = new int[_maxEntries];
		
		// TODO gzip the output stream, as it's going to have a lot of compression
		_payloadOut = new DrumDataOutput(new FileOutputStream(File.createTempFile("drum-payload", "bin")));
		
		// TODO set these numbers based on the maxEntries. I got these from http://hur.st/bloomfilter,
		// using 1M entries and a 1-in-100K probability of a false positive. With 1M entries and a 1% dup
		// rate it returns a high dup count (10001 or 10002, in all my tests) about once very 10 times.
		// Reducing the number of hash functions speeds this up by a bit (e.g. 10 makes it about 20%
		// faster, but with slightly higher false positive rates). But that means we're using about 3
		// bytes/entry for the Bloom set, versus about 16 bytes/entry (estimated) for the other data,
		// so this adds about 19% to the size, in exchange for skipping a scan at the end.
		_dupTracker = new BloomFilter(26_000_000, 17);
		_numEstimatedDups = 0;
	}
	
	public int size() {
		// TODO add in size of entries that have merged/spilled to disk.
		return _numEntries;
	}
	
	// TODO use a CrawlDbUrl as the one parameter, call it to get key, value, payload
	public boolean add(long key, Object value, Payload payload) throws IOException {
		
		_keys[_numEntries] = key;
		_values[_numEntries] = value;
		_offsets[_numEntries] = writePayload(payload);

		_numEntries += 1;

		// Keep track of potential dups with BloomSet(key)
		if (_dupTracker.membershipTest(key)) {
			_numEstimatedDups += 1;
		} else {
			_dupTracker.add(key);
		}

		if (_numEntries >= _maxEntries) {
			merge();
			return true;
		} else {
			return false;
		}
	}

	/**
	 * If we have a payload, write it out to our payload file and return
	 * the offset.
	 * 
	 * @param payload
	 * @return offset of data written, or NO_PAYLOAD_OFFSET
	 */
	private int writePayload(Payload payload) throws IOException {
		if (payload == null) {
			return NO_PAYLOAD_OFFSET;
		}
		
		// Write the payload to our file, and return the offset of the data
		int result = _payloadOut.getBytesWritten();
		payload.write(_payloadOut);
		return result;
	}

	/**
	 * Merge our in-memory array and related payload file with the persisted version
	 */
	private void merge() {
		DrumMapSorter.quickSort(_keys, 0, _numEntries - 1, _offsets, _values);

		
		if (_numEstimatedDups > (_numEntries/100)) {
			for (int i = 1; i < _numEntries; i++) {
				if (_keys[i - 1] == _keys[i]) {
					// TODO merge lower entry into upper entry
					// TODO mark lower entry as unused?
					// Hmm, we could maybe skip this, since we're doing a merge anyway
					// with what's on disk. Or maybe see how many dups we get, and if
					// they're too high then "fill the holes" from the back forward
					// (more efficient, since we won't spend time doing this for
					// entries that will be merged). We could track number of potential
					// dups via a BloomSet, so only bother with the dedup here if it's
					// likely to be useful.
				}
			}
		}
		
		// TODO do the merge
		
	}

	@Override
	public void close() throws IOException {
		// TODO flush as needed.
		
		_payloadOut.close();
	}

	
	/**
	 * Return the value from the in-memory array for <key>. If payload isn't null,
	 * fill it in with the payload for this entry, or clear it if there's no payload.
	 * 
	 * @param key
	 * @param payload
	 * @return
	 */
	public Object getInMemoryEntry(long key, Payload payload) throws IOException {
		int index = findKey(key);
		if (index < 0) {
			if (payload != null) {
				payload.clear();
			}
			
			return null;
		} else {
			if (payload != null) {
				int payloadOffset = _offsets[index];
				if (payloadOffset == NO_PAYLOAD_OFFSET) {
					payload.clear();
				} else {
					// TODO fill in payload. This means reading from the file.
					// So I think we might need a flush() call, which triggers
					// no more updates? Or maybe this is a test-only call, so
					// we can open up the file each time, seek to the target
					// offset, and read the data.
					_payloadOut.flush();
					
				}
			}
			return _values[index];
		}
	}

	private int findKey(long key) {
		int index = -1;
//		if (_sortedEntries > 0) {
//			index = Arrays.binarySearch(_keys, 0, _sortedEntries, key);
//		}
//		
//		if (index < 0) {
//			for (int i = _sortedEntries; i < _numEntries; i++) {
//				if (_keys[i] == key) {
//					index = i;
//					break;
//				}
//			}
//		}
		
		return index;
	}
	
}
