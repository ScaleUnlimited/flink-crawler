package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * A DrumMap implements the DRUM storage system as described by the IRLBot paper
 * (see http://irl.cs.tamu.edu/people/hsin-tsang/papers/www2008.pdf)
 * 
 * There's an in-memory array of new key/value pairs, where the key is an 8-byte
 * (long) hash, the value is 16 bytes of additional data, and there's an
 * additional 8-byte offset into a separate "payload" file, where associated data
 * is kept. For our use case the hash is generated from the URL, the value is
 * the URL status, score(s), next fetch time, and the payload is the actual URL.
 * 
 * So each entry is 32 bytes of data. This of course requires that the value
 * (URL status info) fits in 16 bytes - if not, we'd have to expand this.
 * 
 * We store this data in a single large byte[], and have a separate
 * int[] of offsets into the 32-byte "entries" stored in the byte array. This
 * array of int offsets is what we sort (by hash) before doing  merge (see below).
 * We don't bother doing any de-duplication while adding values; this happens
 * during the merge.
 * 
 * A merge is triggered when our array is full, or more URLs are needed for
 * fetching, or we're shutting down and have to persist state. The merge
 * process first sorts the in-memory array by hash, then (if it exists) does
 * a merge with the (sorted) "active" URLs data stored on disk as two files;
 * one is for the key/value map, and the other is for the payload. Since the
 * "active" URLs data is also sorted, the merge allocates a small buffer that
 * is used to read in chunks of the key/value pairs, which are then merged
 * with the in-memory URLs.
 * 
 * A "merger" class is called to handle URL merging. The result of the merge
 * is classified as either "fetch" (add to fetch queue and put in active list),
 * "active" (only put in active list), or "archive" (add to separate archive
 * data). We archive URLs that have too low of a score (can be calculated many
 * ways) so that they don't clog up processing; this lets us keep the active
 * data set to a reasonable size, so merging doesn't take forever.
 * 
 * The score threshold for fetch vs. active vs. archive is something we probably
 * want to make dynamic. We'd start with a guesstimate, and adjust as we process
 * entries to give us the target max # of entries to fetch vs keep active vs.
 * archive (or could be percentages).
 * 
 * During this merge the DrumMap is still usable, as a new in-memory array
 * is allocated, along with a new payload file.
 * 
 * TODO - we could make the data variable size (adds a byte for the length), so
 * that when we get a new URL (outlink) it's stored with just the hash and the
 * offset into the payload file (thus taking up less space).
 * 
 */
public class DrumMap implements Closeable {
	private static final int NO_VALUE_INDEX = -1;
	private static final int NO_PAYLOAD_OFFSET = -1;
	
	public static int DEFAULT_MAX_ENTRIES = 10_000;

	// Max number of entries in the in-memory array
	private int _maxEntries;
	
	// Number of entries in the in-memory arrays.
	private int _numEntries;
	
	// An in-memory array of keys (hashes), which are longs
	private long[] _keys;
	
	// An in-memory array of values, which are generic Objects
	private Object[] _values;
	
	// An in-memory array of offsets into the payload file
	private int[] _offsets;
	
	// File where the payload data is being kept
	private File _payloadFile;
	
	private DrumDataOutput _payloadOut;
	
	public DrumMap(int maxEntries) throws FileNotFoundException, IOException {
		_maxEntries = maxEntries;
		_numEntries = 0;
		
		_keys = new long[_maxEntries];
		_values = new Object[_maxEntries];
		_offsets = new int[_maxEntries];
		
		// TODO gzip the output stream, as it's going to have a lot of compression. But
		// can we keep track of offsets then? Do these need to be bit offsets? Or maybe
		// since we only should be walking through the entries linearly, we could just
		// use an entry id (0...n) vs. an offset, so we don't need locations.
		_payloadFile = File.createTempFile("drum-payload", "bin");
		_payloadOut = new DrumDataOutput(new FileOutputStream(_payloadFile));
	}
	
	public int size() {
		// TODO add in size of entries that have merged/spilled to disk.
		return _numEntries;
	}
	
	public boolean add(long key, Object value, IPayload payload) throws IOException {
		
		_keys[_numEntries] = key;
		_values[_numEntries] = value;
		_offsets[_numEntries] = writePayload(payload);

		_numEntries += 1;

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
	private int writePayload(IPayload payload) throws IOException {
		if (payload == null) {
			return NO_PAYLOAD_OFFSET;
		} else {
			// Write the payload to our file, and return the offset of the data
			int result = _payloadOut.getBytesWritten();
			payload.write(_payloadOut);
			return result;
		}
	}

	/**
	 * Merge our in-memory array and related payload file with the persisted version
	 * 
	 * TODO - take everything here we need to do the merge, or provide that up above?
	 */
	public void merge() {
		DrumMapSorter.quickSort(_keys, 0, _numEntries - 1, _offsets, _values);

		
		// TODO do the merge
		
	}

	@Override
	public void close() throws IOException {
		if (_payloadOut == null) {
			throw new IllegalStateException("Already closed!");
		}
		
		_payloadOut.close();
		_payloadOut = null;
	}

	
	/**
	 * Return the value from the in-memory array for <key>. If payload isn't null,
	 * fill it in with the payload for this entry, or clear it if there's no payload.
	 * WARNING!!! This is only used for testing!
	 * 
	 * @param key
	 * @param payload
	 * @return
	 */
	public Object getInMemoryEntry(long key, IPayload payload) throws IOException {
		if (_payloadOut != null) {
			throw new IllegalStateException("Must be closed first!");
		}

		int index = findKey(key);
		if (index < 0) {
			if (payload != null) {
				payload.clear();
			}
			
			return null;
		} else {
			if (payload != null) {
				int payloadOffset = _offsets[index];
				// TODO fill in payload. This means reading from the file.
				// So I think we might need a flush() call, which triggers
				// no more updates? Or maybe this is a test-only call, so
				// we can open up the file each time, seek to the target
				// offset, and read the data.
				FileInputStream fis = new FileInputStream(_payloadFile);
				DataInputStream dis = new DataInputStream(fis);
				dis.skip(payloadOffset);
				payload.readFields(dis);
				dis.close();
			}
			
			return _values[index];
		}
	}

	private int findKey(long key) {
		for (int i = 0; i < _numEntries; i++) {
			if (_keys[i] == key) {
				return i;
			}
		}
		
		return -1;
	}

	
}
