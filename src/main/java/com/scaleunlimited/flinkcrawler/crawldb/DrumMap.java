package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import com.scaleunlimited.flinkcrawler.utils.ByteUtils;

/**
 * A DrumMap implements a form of DRUM storage system as described by the IRLBot paper
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
 * array of int offsets is what we sort (by hash) before doing a merge (see below).
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
 */
public class DrumMap implements Closeable {
	private static final int NO_PAYLOAD_OFFSET = -1;
	
	public static final int DEFAULT_MAX_ENTRIES = 10_000;

	private static final int VALUE_SIZE = 15;
	private static final int ENTRY_SIZE = 8 + 8 + 1 + VALUE_SIZE;
	
	private static final String PAYLOAD_FILENAME = "payload.bin";
	
	// Max number of entries in the in-memory array
	private int _maxEntries;
	
	// Number of entries in the in-memory arrays.
	private int _numEntries;
	
	// Each entry has an offset into _entryData, where the
	// hash (8 bytes), payload offset (8 bytes), and value
	// (16 bytes) are stored.
	private int[] _entries;
	private byte[] _entryData;
	private int _entryDataOffset;
	
	// Location for payload, spilled entries files.
	private File _dataDir;
	
	// File where the payload data is being kept
	private File _payloadFile;
	
	private DrumDataOutput _payloadOut;
	
	public DrumMap(int maxEntries, File dataDir) throws FileNotFoundException, IOException {
		_maxEntries = maxEntries;
		_numEntries = 0;
		
		_entries = new int[_maxEntries];
		_entryData = new byte[ENTRY_SIZE * _maxEntries];
		_entryDataOffset = 0;
		
		_dataDir = dataDir;
	}
	
	public void open() throws IOException {
		FileUtils.forceMkdir(_dataDir);
		
		// FUTURE gzip the output stream, as it's going to have a lot of compression. But
		// can we keep track of offsets then? Do these need to be bit offsets? Or maybe
		// since we only should be walking through the entries linearly, we could just
		// use an entry id (0...n) vs. an offset, so we don't need locations.
		_payloadFile = new File(_dataDir, PAYLOAD_FILENAME);
		_payloadFile.delete();
		_payloadFile.createNewFile();
		
		_payloadOut = new DrumDataOutput(new FileOutputStream(_payloadFile));
	}
	
	public int size() {
		// TODO add in size of entries that have merged/spilled to disk.
		return _numEntries;
	}
	
	public boolean add(long key, byte[] value, IPayload payload) throws IOException {
		if (value.length > VALUE_SIZE) {
			throw new IllegalArgumentException("value length is too long, max is " + VALUE_SIZE);
		}
		
		_entries[_numEntries++] = _entryDataOffset;
		ByteUtils.longToBytes(key, _entryData, _entryDataOffset);
		_entryDataOffset += 8;
		ByteUtils.longToBytes(writePayload(payload), _entryData, _entryDataOffset);
		_entryDataOffset += 8;
		
		_entryData[_entryDataOffset++] = (byte)value.length;
		if (value.length > 0) {
			System.arraycopy(value, 0, _entryData, _entryDataOffset, value.length);
			_entryDataOffset += value.length;
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
	private long writePayload(IPayload payload) throws IOException {
		if (payload == null) {
			return NO_PAYLOAD_OFFSET;
		} else {
			// Write the payload to our file, and return the offset of the data
			long result = _payloadOut.getBytesWritten();
			payload.write(_payloadOut);
			return result;
		}
	}

	public void sort() {
		DrumMapSorter.quickSort(_entries, 0, _numEntries - 1, _entryData);
	}
	
	/**
	 * Merge our in-memory array and related payload file with the persisted version
	 * 
	 * TODO - take everything here we need to do the merge, or provide that up above?
	 */
	public void merge() {
		sort();
		
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
	public boolean getInMemoryEntry(long key, byte[] value, IPayload payload) throws IOException {
		if (_payloadOut != null) {
			throw new IllegalStateException("Must be closed first!");
		}

		if (value.length > VALUE_SIZE) {
			throw new IllegalArgumentException("value length is too long, max is " + VALUE_SIZE);
		}

		int index = findKey(key);
		if (index < 0) {
			if (payload != null) {
				payload.clear();
			}
			
			return false;
		} else {
			int entryDataOffset = _entries[index];
			entryDataOffset += 8;
			
			if (payload != null) {
				long payloadOffset = ByteUtils.bytesToLong(_entryData, entryDataOffset);
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
			
			entryDataOffset += 8;
			int valueLength = (int)_entryData[entryDataOffset++];
			if (valueLength > 0) {
				System.arraycopy(_entryData, entryDataOffset, value, 0, valueLength);
			}
			
			return true;
		}
	}

	private int findKey(long key) {
		for (int i = 0; i < _numEntries; i++) {
			if (ByteUtils.bytesToLong(_entryData, _entries[i]) == key) {
				return i;
			}
		}
		
		return -1;
	}

	
}
