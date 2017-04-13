package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger.MergeResult;
import com.scaleunlimited.flinkcrawler.crawldb.DrumMapFile.DrumMapFileType;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.utils.ByteUtils;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue.MergeStatus;
import com.scaleunlimited.flinkcrawler.utils.IoUtils;

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
 * same URL (actually two entries with the same hash, based on the URL).
 * 
 * We store the hash/value/payload offset entries in a single large byte[], and have a separate
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
 * A merge is triggered when our int[] or byte[] arrays are full, or more URLs are needed for
 * fetching, or we're shutting down and have to persist state. The merge
 * process first sorts the in-memory array by hash, then does
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

	public static final int MAX_VALUE_LENGTH = CrawlStateUrl.maxValueLength();
	
	// Long hash, long payload offset, value length byte.
	private static final int NON_VALUE_SIZE = 8 + 8 + 1;
	
	// Max entry size is the above fixed size plus the max value length.
	private static final int MAX_ENTRY_SIZE = NON_VALUE_SIZE + MAX_VALUE_LENGTH;

	protected static final String WORKING_SUBDIR_NAME = "working";
	private static final String NEXT_SUBDIR_NAME = "next";
	private static final String TEMP_SUBDIR_NAME = "temp";

	protected static final String MEMORY_FILE_PREFIX = "memory";
	protected static final String ACTIVE_FILE_PREFIX = "active";
	protected static final String ARCHIVED_FILE_PREFIX = "archived";
	
	private BaseCrawlDBMerger _merger;
	
	// Max number of entries in _entries.
	private int _maxEntries;
	
	// Current number of entries in _entries.
	private int _numEntries;
	
	// Each entry has an offset into _entryData, where the
	// hash (8 bytes), payload offset (8 bytes), and value
	// (1+n bytes) are stored.
	private int[] _entries;
	
	// Current offset into _entryData
	private int _entryDataOffset;
	private byte[] _entryData;
	
	// Location for payload, spilled entries files, etc.
	private File _dataDir;
	
	// File for the memory payload data.
	private DrumPayloadFile _memoryPayloadFile;
	
	// Files for the active & archived key-value & payload data
	private DrumMapFile _activeFile;
	private DrumMapFile _archivedFile;
	
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
		
		File workingDir = new File(_dataDir, WORKING_SUBDIR_NAME);
		FileUtils.forceMkdir(workingDir);
		
		reset(workingDir, null);
	}
	
	@Override
	public void close() throws IOException {
		if (_memoryPayloadFile == null) {
			throw new IllegalStateException("Already closed!");
		}
		
		// TODO need to force a merge with active URLs, write out results.
		// But pass in a FetchQueue that never "takes" any URLs, so
		// status is never saved as "FETCHING".
		
		IoUtils.closeAll(_memoryPayloadFile, _activeFile, _archivedFile);
		_memoryPayloadFile = null;
		_activeFile = null;
		_archivedFile = null;
	}

	/**
	 * Create and open the payload file, and resets current positions
	 * 
	 * @throws IOException
	 */
	private void reset(File workingDir, File nextDir) throws IOException {
		
		// FUTURE - figure out best way to back out of a partial reset, if
		// something goes wrong mid-way.
		
		if (nextDir != null) {
			File tempDir = new File(_dataDir, TEMP_SUBDIR_NAME);
			FileUtils.forceDelete(tempDir);
			FileUtils.moveDirectory(workingDir, tempDir);
			
			FileUtils.moveDirectory(nextDir, workingDir);
			
			// TODO move/rename the archived key-value and payload files from the
			// old working dir (now tempDir) into the new working dir. They'll get
			// a timestamped name.
			
			// All done with the temp directory, delete it.
			FileUtils.forceDelete(tempDir);
		} else {
			// Create active & archived keyvalue/payload files, but only if they don't exist.
			_activeFile = new DrumMapFile(workingDir, ACTIVE_FILE_PREFIX, false);
			_archivedFile = new DrumMapFile(workingDir, ARCHIVED_FILE_PREFIX, false);
		}
		
		// Now that we have the active & archived files set up, reset the memory key-value 
		// data & payload file.
		_numEntries = 0;
		_entryDataOffset = 0;
		
		// Create a new memory payload file. If the file already exists, it's OK
		// to delete it, as we should already have processed everything inside
		// of it.
		String payloadFilename = DrumMapFile.makeFilename(MEMORY_FILE_PREFIX, DrumMapFileType.PAYLOAD);
		_memoryPayloadFile = new DrumPayloadFile(new File(workingDir,  payloadFilename));
		_memoryPayloadFile.create();
	}

	/**
	 * @return count of entries in memory.
	 */
	public int size() {
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
		long result = _memoryPayloadFile.getBytesWritten();
		payload.write(_memoryPayloadFile.getDrumDataOutput());
		LOGGER.trace(String.format("Wrote payload at offset %d", result));
		return result;
	}

	/**
	 * Order entries in the int[] array by hash.
	 */
	protected void sort() {
		DrumMapSorter.quickSort(_entries, 0, _numEntries - 1, _entryData);
	}
	
	/**
	 * Merge our in-memory array and related payload file with the KV data/payload files
	 * for active entries.
	 * 
	 */
	public void merge(FetchQueue queue) {
		// Get a lock so that no new entries are added while we're merging.
		synchronized (_lock) {
			DrumKVIterator memoryIterator = null;
			DrumKVIterator activeIterator = null;
			File nextDir = null;
			
			try {
				// If we have no active data, and no in-memory data, then we're all done.
				if ((size() == 0) && _activeFile.isEmpty()) {
					return;
				}
				
				sort();
				
				// Create memory and disk KV iterators. The MemoryDrumKVIterator needs the merger to handle
				// duplicate entries.
				memoryIterator = new MemoryDrumKVIterator(_merger, _numEntries, _entries, _entryData, _memoryPayloadFile);
				activeIterator = new DiskDrumKVIterator(_activeFile);
				nextDir = doMerge(queue, memoryIterator, activeIterator);
				
				_mergeNeeded = false;
				
				// Set up the new working directory to be nextDir. If reset() fails and throws an exception, we
				// assume that it won't swap in the new directory, so we have to get rid of it.
				reset(new File(_dataDir, WORKING_SUBDIR_NAME), nextDir);
			} catch (IOException e) {
				FileUtils.deleteQuietly(nextDir);
				
				throw new RuntimeException(Thread.currentThread().getName() + ": Exception while merging: " + e.getMessage(), e);
			} finally {
				IoUtils.closeAllQuietly(memoryIterator, activeIterator);
			}
		}
	}

	/**
	 * Merge the memory KV data & payload file with the active KV file & payload files
	 * 
	 * @param queue - where to add the entries
	 * @param memKVIter - iterator for in-memory KV data & payload file
	 * @param diskKVIter - iterator for active KV/payload files
	 * @return Directory where we've written the latest active/archive data, and the new in-memory payload file
	 * @throws IOException
	 */
	private File doMerge(FetchQueue queue, DrumKVIterator memKVIter, DrumKVIterator diskKVIter) throws IOException {
		// Set up location for new active & archive file data.
		File nextDir = new File(_dataDir, NEXT_SUBDIR_NAME);
		nextDir.mkdir();
		
		// Create the next active and archived key-value/payload files.
		DrumMapFile nextActiveFile = new DrumMapFile(nextDir, ACTIVE_FILE_PREFIX, true);
		DrumMapFile nextArchivedFile = new DrumMapFile(nextDir, ARCHIVED_FILE_PREFIX, true);
		
		boolean hasMem = false;
		boolean hasDisk = false;
		boolean firstLoop = true;
		
		DrumKeyValue memKV = null;
		DrumKeyValue diskKV = null;
		DrumKeyValue curKV = null;
		DrumKeyValue nextKV = null;
		
		DrumKeyValue mergedKV = new DrumKeyValue();
		
		RandomAccessFile memPayloadRAF = memKVIter.getPayloadRAF();
		RandomAccessFile diskPayloadRAF = diskKVIter.getPayloadRAF();
		RandomAccessFile curPayloadRAF = null;
		RandomAccessFile nextPayloadRAF = null;
		
		while (true) {
			if (!hasMem) {
				// Reload A
				hasMem = memKVIter.hasNext();
				if (hasMem) {
					memKV = memKVIter.next(memKV);
				} else {
					memKV = null;
				}
			}
			
			if (!hasDisk) {
				// Reload B
				hasDisk = diskKVIter.hasNext();
				if (hasDisk) {
					diskKV = diskKVIter.next(diskKV);
				} else {
					diskKV = null;
				}
			}
			
			if (!hasDisk) {
				if (!hasMem) {
					// Emit the current value if we have one.
					if (curKV != null) {
						addEntry(nextActiveFile, nextArchivedFile, queue, curKV, curPayloadRAF);
					}
					
					break;
				} else {
					nextKV = memKV;
					nextPayloadRAF = memPayloadRAF;
					hasMem = false;
				}
			} else if (!hasMem) {
				// We know we must have B, otherwise !hasB would have
				// evaluated true above.
				nextKV = diskKV;
				nextPayloadRAF = diskPayloadRAF;
				hasDisk = false;
			} else if (memKV.getKeyHash() < diskKV.getKeyHash()) {
				nextKV = memKV;
				nextPayloadRAF = memPayloadRAF;
				hasMem = false;
			} else if (diskKV.getKeyHash() < memKV.getKeyHash()) {
				nextKV = diskKV;
				nextPayloadRAF = diskPayloadRAF;
				hasDisk = false;
			} else {
				// TODO merge the two values.
				nextKV = null; // TODO Set to merged result. make sure nextKV has offset that's appropriate for memPayloadRAF.
				nextPayloadRAF = memPayloadRAF;
				hasMem = hasDisk = false;
			}
			
			// TODO is this even needed? Based on above, we will be working our way through mem & disk entries,
			// always picking the lower hash. If the hashes are the same, then we merge. The disk file doesn't have
			// any dups, and the iterator for the in-memory has taken care of merging. 
			// Compare the nextKV with current hash. If different, emit
			// current value, otherwise merge.
			if (curKV.getKeyHash() != nextKV.getKeyHash()) {
				// We can emit curKV, and then set that to the nextKV.
				// TODO make it so.
			} else {
				// TODO merge them. Will they always have different hashes?
			}
			
			firstLoop = false;
		}
		
		return nextDir;
		
		/*
		byte[] oldValueA = new byte[1 + MAX_VALUE_LENGTH];
		byte[] newValueA = new byte[1 + MAX_VALUE_LENGTH];
		byte[] mergedValueA = new byte[1 + MAX_VALUE_LENGTH];

		byte[] curValueA = null;
		long nextHashA = 0;

		for (int i = 0; i < _numEntries; i++) {
			// Use next hash if we've got it.
			long curHash;

			if (i == 0) {
				curHash = getHash(0);
				System.arraycopy(_entryData, getValueOffset(0), oldValueA, 0, getValueSize(0));
				curValueA = oldValueA;
			} else {
				curHash = nextHashA;
				// curValue has been set up in previous loop.
			}

			// If we're at the end, fake a hash that will never match, to trigger
			// processing of the last entry.
			boolean lastEntry = i == _numEntries - 1;
			nextHashA = lastEntry ? curHash + 1 : getHash(i + 1);

			if (nextHashA != curHash) {
				// Current entry can be processed, since next entry is different.
				// See if we need to skip over some number of entries from active.
				// TODO make it so
				// See if we need to merge with an entry from active
				// TODO make it so.
				// Decide what to do with the merged result
				MergedStatus status = _merger.getMergedStatus(curValueA);
				if (status == MergedStatus.ACTIVE_FETCH) {
					long position = getPayloadPosition(i);
					LOGGER.trace(String.format("%s: Seeking to position %d for entry #%d", Thread.currentThread().getName(), position, i));
					memoryPayload.seek(position);
					CrawlStateUrl url = new CrawlStateUrl();
					url.readFields(memoryPayload);
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
					System.arraycopy(_entryData, getValueOffset(i + 1), oldValueA, 0, getValueSize(i + 1));
					curValueA = oldValueA;
					curHash = nextHashA;
				}
			} else {
				// we have two entries with the same hash, so we have to merge them.
				// FUTURE remove need to copy to oldValue, newValue by having byte[], offset params
				System.arraycopy(_entryData, getValueOffset(i + 1), newValueA, 0, getValueSize(i + 1));
				MergeResult result = _merger.doMerge(curValueA, newValueA, mergedValueA);

				if (result == MergeResult.USE_OLD) {
					curValueA = oldValueA;
				} else if (result == MergeResult.USE_NEW) {
					curValueA = newValueA;
				} else if (result == MergeResult.USE_MERGED) {
					curValueA = mergedValueA;
				} else {
					throw new RuntimeException("Unknown merge result: " + result);
				}
			}
		}
		*/
		
	}
	
	private void addEntry(DrumMapFile activeFile, DrumMapFile archivedFile,
			FetchQueue queue, DrumKeyValue dkv, RandomAccessFile payloadRAF) throws IOException {
		CrawlStateUrl url = CrawlStateUrl.fromKV(dkv, payloadRAF);
		MergeStatus status = queue.add(url);
		switch (status) {
		case ACTIVE:
			// TODO update dkv with offset in payload file
			dkv.write(activeFile.getKeyValueFile());
			url.write(activeFile.getPayloadFile().getRandomAccessFile());
			break;
			
		case ARCHIVE:
			// TODO update dkv with offset in payload file
			dkv.write(archivedFile.getKeyValueFile());
			url.write(archivedFile.getPayloadFile().getRandomAccessFile());
			break;
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
	
	/**
	 * Return the value from the in-memory array for <key>. If payload isn't null,
	 * fill it in with the payload for this entry.
	 * 
	 * WARNING!!! This is only used for testing!
	 * 
	 * @param key
	 * @param payload
	 * @return
	 */
	public boolean getInMemoryEntry(long key, byte[] value, IPayload payload) throws IOException {
		
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
				RandomAccessFile memoryPayloadFile = _memoryPayloadFile.getRandomAccessFile();
				byte[] payloadData = new byte[(int)memoryPayloadFile.length()];
				memoryPayloadFile.read(payloadData);

				long payloadOffset = ByteUtils.bytesToLong(_entryData, entryDataOffset);
				
				ByteArrayInputStream bais = new ByteArrayInputStream(payloadData);
				DataInputStream dis = new DataInputStream(bais);
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

	private static enum PayloadFileType {
		MEMORY,
		ACTIVE,
		ARCHIVE
	}
}
