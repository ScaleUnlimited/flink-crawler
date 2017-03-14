package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger.MergeResult;
import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger.MergedStatus;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.ValidUrl;
import com.scaleunlimited.flinkcrawler.utils.ByteUtils;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;

/**
 * A DrumMap implements a form of DRUM storage system as described by the IRLBot paper
 * (see http://irl.cs.tamu.edu/people/hsin-tsang/papers/www2008.pdf)
 * 
 * There's an in-memory array of key/value pairs, where the key is an 8-byte
 * (long) hash, the value is up to 255 bytes of additional data, and there's an
 * additional 8-byte offset into a separate "payload" file, where associated data
 * is kept. For our use case the hash is generated from the URL, the value is
 * the URL status, score(s), next fetch time, etc and the payload is the actual URL.
 * 
 * The value must contain everything needed to merge two or more entries with the
 * same URL (actually same hash).
 * 
 * We store this data in a single large byte[], and have a separate
 * int[] of offsets into the "entries" stored in the byte array. This
 * array of int offsets is what we sort (by hash) before doing a merge (see below).
 * We don't bother doing any de-duplication while adding values; this happens
 * during the merge.
 * 
 * Note that this approach maximizes the number of records that fit into memory,
 * and avoids lots of object allocation, at the expense of longer times to add
 * (and sort) entries. For 1M entries, if we just used arrays for the keys,
 * the values, and the payload offsets, then it's 200ms or so to add them,
 * versus 900ms for the approach we're using. But the per-entry time is so
 * small that this isn't really much of a factor.
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
	private static Logger LOGGER = LoggerFactory.getLogger(DrumMap.class);
	
	public static final int DEFAULT_MAX_ENTRIES = 10_000;

	public static final int MAX_VALUE_LENGTH = 255;
	
	// Long hash, long payload offset, value length byte.
	private static final int NON_VALUE_SIZE = 8 + 8 + 1;
	private static final int MAX_ENTRY_SIZE = NON_VALUE_SIZE + MAX_VALUE_LENGTH;
	
	private BaseCrawlDBMerger _merger;
	
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
	
	private volatile boolean _mergeNeeded = false;
	
	private AtomicBoolean _lock;
	
	public DrumMap(int maxEntries, int averageValueLength, File dataDir, BaseCrawlDBMerger merger) throws FileNotFoundException, IOException {
		_maxEntries = maxEntries;
		_entries = new int[_maxEntries];
		
		final int averageEntrySize = NON_VALUE_SIZE + averageValueLength;
		_entryData = new byte[averageEntrySize * _maxEntries];
		
		_dataDir = dataDir;
		
		_merger = merger;
		
		_lock = new AtomicBoolean(false);
	}
	
	public void open() throws IOException {
		FileUtils.forceMkdir(_dataDir);
		
		reset();
	}
	
	/**
	 * Create and open the payload file, and resets current positions
	 * 
	 * @throws IOException
	 */
	private void reset() throws IOException {
		_numEntries = 0;
		_entryDataOffset = 0;

		_payloadFile = new File(_dataDir, makePayloadFilename());
		_payloadFile.delete();
		_payloadFile.createNewFile();
		
		_payloadOut = new DrumDataOutput(new FileOutputStream(_payloadFile));
	}

	private String makePayloadFilename() {
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSS");
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return String.format("payload-%s.bin", dateFormat.format(new Date()));
	}

	public int size() {
		// TODO add in size of entries that have merged/spilled to disk?
		return _numEntries;
	}
	
	public boolean add(long key, byte[] value, IPayload payload) throws IOException {
		synchronized (_lock) {
			if (_mergeNeeded) {
				throw new IllegalStateException("Merge needed before add can be called");
			}

			int valueLength = ((value == null) || (value.length == 0) ? 0 : value[0]);
			if (valueLength > MAX_VALUE_LENGTH) {
				throw new IllegalArgumentException("value length is too long, max is " + MAX_VALUE_LENGTH);
			}

			_entries[_numEntries++] = _entryDataOffset;
			ByteUtils.longToBytes(key, _entryData, _entryDataOffset);
			_entryDataOffset += 8;
			ByteUtils.longToBytes(writePayload(payload), _entryData, _entryDataOffset);
			_entryDataOffset += 8;

			_entryData[_entryDataOffset++] = (byte)valueLength;
			if (valueLength > 0) {
				System.arraycopy(value, 1, _entryData, _entryDataOffset, valueLength);
				_entryDataOffset += valueLength;
			}

			// If we're within the max entry size of the end of _entryData, we need a merge.
			// Same if we're at the limit of entries in our offset array.
			_mergeNeeded = (_numEntries >= _maxEntries) || ((_entryDataOffset + MAX_ENTRY_SIZE) > _entryData.length);
			return _mergeNeeded;
		}
	}

	/**
	 * Write the payload out to our payload file and return
	 * the offset.
	 * 
	 * @param payload
	 * @return offset of data written
	 */
	private long writePayload(IPayload payload) throws IOException {
		// Write the payload to our file, and return the offset of the data
		long result = _payloadOut.getBytesWritten();
		payload.write(_payloadOut);
		LOGGER.info(String.format("Wrote payload at offset %d", result));
		return result;
	}

	public void sort() {
		DrumMapSorter.quickSort(_entries, 0, _numEntries - 1, _entryData);
	}
	
	/**
	 * Merge our in-memory array and related payload file with the persisted version
	 * 
	 */
	public void merge(FetchQueue queue) {
		// Get a lock so that no new entries are added while we're merging.
		synchronized (_lock) {
			try {
				sort();

				if (onDiskActiveExists()) {
					doMergeWithDisk(queue);
				} else {
					doMergeMemoryOnly(queue);
				}

				_mergeNeeded = false;
			} catch (IOException e) {
				throw new RuntimeException("Exception while merging: " + e.getMessage(), e);
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (_payloadOut == null) {
			throw new IllegalStateException("Already closed!");
		}
		
		// TODO need to force a merge with active URLs, write out results.
		
		_payloadOut.close();
		_payloadOut = null;
	}

	private void doMergeWithDisk(FetchQueue queue) {
		// TODO do the merge with the on-disk data
		
	}
	
	/**
	 * There's no active disk data, so we're just doing a self-merge with
	 * what's in the memory list.
	 * 
	 * @param queue
	 * @throws IOException 
	 */
	private void doMergeMemoryOnly(FetchQueue queue) throws IOException {
		if (_numEntries == 0) {
			return;
		}
		
		_payloadOut.close();
		_payloadOut = null;
		RandomAccessFile dis = new RandomAccessFile(_payloadFile, "r");

		try {
			byte[] oldValue = new byte[CrawlStateUrl.maxValueSize()];
			byte[] newValue = new byte[CrawlStateUrl.maxValueSize()];
			byte[] mergedValue = new byte[CrawlStateUrl.maxValueSize()];

			byte[] curValue = null;
			long nextHash = 0;

			for (int i = 0; i < _numEntries; i++) {
				// Use next hash if we've got it.
				long curHash;

				if (i == 0) {
					curHash = getHash(0);
					System.arraycopy(_entryData, getValueOffset(0), oldValue, 0, getValueSize(0));
					curValue = oldValue;
				} else {
					curHash = nextHash;
					// curValue has been set up in previous loop.
				}

				// If we're at the end, fake a hash that will never match, to trigger
				// processing of the last entry.
				boolean lastEntry = i == _numEntries - 1;
				nextHash = lastEntry ? curHash + 1 : getHash(i + 1);

				if (nextHash != curHash) {
					// Current entry can be processed, since next entry is different.
					// Decide what to do with the merged result
					MergedStatus status = _merger.getMergedStatus(curValue);
					if (status == MergedStatus.ACTIVE_FETCH) {
						long position = getPayloadPosition(i);
						LOGGER.info(String.format("%s: Seeking to position %d for entry #%d", Thread.currentThread().getName(), position, i));
						dis.seek(position);
						CrawlStateUrl url = new CrawlStateUrl();
						url.readFields(dis);
						queue.add(url);
						
						// TODO - Also put in active disk
					} else if (status == MergedStatus.ACTIVE) {
						// TODO just put in active disk
					} else if (status == MergedStatus.ARCHIVE) {
						// TODO put in archive
					} else {
						throw new RuntimeException("Unknown merge status: " + status);
					}

					if (!lastEntry) {
						// FUTURE remove need to copy by having getMergedStatus(byte[], offset)
						System.arraycopy(_entryData, getValueOffset(i + 1), oldValue, 0, getValueSize(i + 1));
						curValue = oldValue;
						curHash = nextHash;
					}
				} else {
					// we have two entries with the same hash, so we have to merge them.
					// FUTURE remove need to copy to oldValue, newValue by having byte[], offset params
					System.arraycopy(_entryData, getValueOffset(i + 1), newValue, 0, getValueSize(i + 1));
					MergeResult result = _merger.doMerge(curValue, newValue, mergedValue);

					if (result == MergeResult.USE_OLD) {
						curValue = oldValue;
					} else if (result == MergeResult.USE_NEW) {
						curValue = newValue;
					} else if (result == MergeResult.USE_MERGED) {
						curValue = mergedValue;
					} else {
						throw new RuntimeException("Unknown merge result: " + result);
					}
				}
			}
		} catch (EOFException e) {
			LOGGER.error(Thread.currentThread().getName() + ": EOFException reading from file " + _payloadFile);
		} finally {
			
			// Create a new payload file, reset 
			reset();
			
			IOUtils.closeQuietly(dis);
			
			// TODO delete old payload file if no error?
		}
		
	}
	
	private int getValueOffset(int entryIndex) {
		if ((entryIndex < 0) || (entryIndex >= _numEntries)) {
			throw new IllegalArgumentException("Invalid index: " + entryIndex);
		}
		
		int dataOffset = _entries[entryIndex];
		return dataOffset + 8 + 8;
	}
	
	private int getValueSize(int entryIndex) {
		return 1 + _entryData[getValueOffset(entryIndex)] & 0x00FF;
	}
	
	private long getHash(int entryIndex) {
		if ((entryIndex < 0) || (entryIndex >= _numEntries)) {
			throw new IllegalArgumentException("Invalid index: " + entryIndex);
		}
		
		int dataOffset = _entries[entryIndex];
		return ByteUtils.bytesToLong(_entryData, dataOffset);
	}
	
	private long getPayloadPosition(int entryIndex) {
		if ((entryIndex < 0) || (entryIndex >= _numEntries)) {
			throw new IllegalArgumentException("Invalid index: " + entryIndex);
		}
		
		int dataOffset = _entries[entryIndex];
		return ByteUtils.bytesToLong(_entryData, dataOffset + 8);
	}
	
	private boolean onDiskActiveExists() {
		// TODO check for on disk files (data, payload)
		return false;
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
			
			if (valueLength + 1 > value.length) {
				throw new IllegalArgumentException("value array isn't big enough, we need " + (valueLength + 1));
			}

			value[0] = (byte)valueLength;

			if (valueLength > 0) {
				// Copy over the rest of the value
				System.arraycopy(_entryData, entryDataOffset, value, 1, valueLength);
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
