package com.scaleunlimited.flinkcrawler.utils;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * A DrumMap implements the DRUM storage system as described by the IRLBot paper
 * (see http://irl.cs.tamu.edu/people/hsin-tsang/papers/www2008.pdf)
 * 
 * There's an in-memory array of new key/value pairs, where the key is an 8-byte
 * (long) hash, the value is an (optional) Object of any time, and there's an
 * additional offset into a separate "payload" file, where associated data is kept.
 * 
 * The map is kept semi-sorted. As new keys are added, they get put at the end after
 * the sorted keys, thus existence checks when adding entries use a binary search
 * on the sorted portion, and a linear scan on the unsorted entries. When this list
 * becomes too big, the entire set of entries are re-sorted.
 * 
 * If a key is added that already exists, a provided DrumEntryMerger class is called
 * to merge the old and new entries. The results might be to ignore the new entry,
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
 */
public class DrumMap implements Closeable {
	private static final int NO_PAYLOAD_OFFSET = -1;

	public static int DEFAULT_MAX_ENTRIES = 10_000;

	// Max number of entries in the array
	private int _maxEntries;
	
	// Number of entries in the in-memory arrays.
	private int _numEntries;
	
	// How many of _numEntries are sorted.
	private int _sortedEntries;
	
	// An in-memory array of keys (hashes), which are longs
	private long[] _keys;
	
	// An in-memory array of values, which are generic Objects
	private Object[] _values;
	
	// An in-memory array of offsets into the payload file
	private int[] _offsets;
	
	private DrumDataOutput _payloadOut;
	
	public DrumMap(int maxEntries) throws FileNotFoundException, IOException {
		_maxEntries = maxEntries;
		_numEntries = 0;
		
		_keys = new long[_maxEntries];
		_values = new Object[_maxEntries];
		_offsets = new int[_maxEntries];
		
		_payloadOut = new DrumDataOutput(new FileOutputStream(File.createTempFile("drum-payload", "bin")));
	}
	
	public int size() {
		// TODO add in size of entries that have merged/spilled to disk.
		return _numEntries;
	}
	
	// TODO use a DrumEntry w/key, value, payload
	public boolean add(long key, Object value, Payload payload) throws IOException {
		
		// See if this already exists.
		int index = findKey(key);
		
		if (index < 0) {
			_keys[_numEntries] = key;
			_values[_numEntries] = value;
			
			_offsets[_numEntries] = writePayload(payload);
			
			_numEntries += 1;
			
			if (_numEntries >= _maxEntries) {
				merge();
			} else if (timeToSort()) {
				// Note that "right" side of what to sort is inclusive.
				DrumMapSorter.quickSort(_keys, 0, _numEntries - 1, _offsets, _values);
				_sortedEntries = _numEntries;
			}
			
			return true;
		} else {
			// TODO merge with existing entry
			
			// TODO return different status (eg. new, ignored, merged)
			return false;
		}
	}

	/**
	 * Return true if we should re-sort the data.
	 * 
	 * @return true if we should re-sort.
	 */
	private boolean timeToSort() {
		// TODO use a more sophisticated approach. Basically cost of sort
		// (based on number of sorted & unsorted entries) vs ongoing cost
		// of linear scan.
		
		return _numEntries - _sortedEntries > 100;
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
		// TODO Auto-generated method stub
		
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
		if (_sortedEntries > 0) {
			index = Arrays.binarySearch(_keys, 0, _sortedEntries, key);
		}
		
		if (index < 0) {
			for (int i = _sortedEntries; i < _numEntries; i++) {
				if (_keys[i] == key) {
					index = i;
					break;
				}
			}
		}
		
		return index;
	}
	
}
