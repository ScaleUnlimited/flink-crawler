package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger.MergeResult;
import com.scaleunlimited.flinkcrawler.crawldb.DrumMapFile.DrumMapFileType;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.utils.ByteUtils;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue.UrlState;
import com.scaleunlimited.flinkcrawler.utils.IoUtils;
import com.scaleunlimited.flinkcrawler.utils.OpenFilesUtils;

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

	// Long hash, long payload offset, value length byte.
	private static final int NON_VALUE_SIZE = 8 + 8 + 1;
	
	// Max entry size is the above fixed size plus the max value length.
	private static final int MAX_ENTRY_SIZE = NON_VALUE_SIZE + DrumKeyValue.MAX_VALUE_LENGTH;

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
	
	// Optimization - keep this flag set up (in reset()) so that we can quickly
	// abort merge when there's no data in the working active file.
	private volatile boolean _activeIsEmpty = true;
	
	private volatile boolean _mergeNeeded = false;
	
	private AtomicBoolean _lock;
	
	private OpenFilesUtils _lsof = null; // new OpenFilesUtils();
	
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
		
		IoUtils.closeAll(_memoryPayloadFile /*, _activeFile, _archivedFile*/);
		_memoryPayloadFile = null;
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
			if (tempDir.exists()) {
				FileUtils.forceDelete(tempDir);
			}
			
			FileUtils.moveDirectory(workingDir, tempDir);
			
			FileUtils.moveDirectory(nextDir, workingDir);
			
			// TODO move/rename the archived key-value and payload files from the
			// old working dir (now tempDir) into the new working dir. They'll get
			// a timestamped name.
			

			// All done with the temp directory, delete it.
			FileUtils.forceDelete(tempDir);
		} else {
			// Create active & archived keyvalue/payload files, but only if they don't exist.
			// _activeFile = new DrumMapFile(workingDir, ACTIVE_FILE_PREFIX, false);
			// _archivedFile = new DrumMapFile(workingDir, ARCHIVED_FILE_PREFIX, false);
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
		
		DrumMapFile activeFile = getActiveFile();
		try {
			_activeIsEmpty = activeFile.isEmpty();
		} finally {
			IOUtils.closeQuietly(activeFile);
		}
	}

	/**
	 * @return count of entries in memory.
	 */
	public int size() {
		return _numEntries;
	}
	
	public DrumMapFile getActiveFile() throws IOException {
		return new DrumMapFile(getWorkingDir(), ACTIVE_FILE_PREFIX, false);
	}
	
	public DrumMapFile getArchivedFile() throws IOException {
		return new DrumMapFile(getWorkingDir(), ARCHIVED_FILE_PREFIX, false);
	}
	
	public boolean add(long key, byte[] value, IPayload payload) throws IOException {
		synchronized (_lock) {
			if (_mergeNeeded) {
				throw new IllegalStateException("Merge needed before add can be called");
			}

			int valueLength = ((value == null) || (value.length == 0) ? 0 : value[0]);
			if (valueLength > DrumKeyValue.MAX_VALUE_LENGTH) {
				throw new IllegalArgumentException("value length is too long, max is " + DrumKeyValue.MAX_VALUE_LENGTH);
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
				// If we have no in-memory data and no active data then we're all done.
				if ((size() == 0) && _activeIsEmpty) {
					return;
				}
				
				// displayOpenFiles("Start of merge");
				
				sort();
				
				// Create memory and disk KV iterators. The MemoryDrumKVIterator needs the merger to handle
				// duplicate entries.
				memoryIterator = new MemoryDrumKVIterator(_merger, _numEntries, _entries, _entryData, _memoryPayloadFile);
				activeIterator = new DiskDrumKVIterator(getActiveFile());
				nextDir = doMerge(queue, memoryIterator, activeIterator);
				
				// We have to close the iterators (and thus the files they opened in the directory) before
				// calling reset(), as that moves directories around, after which a close doesn't actually
				// close the files.
				IoUtils.closeAllQuietly(memoryIterator, activeIterator);
				memoryIterator = null;
				activeIterator = null;
				
				// Set up the new working directory to be nextDir. If reset() fails and throws an exception, we
				// assume that it won't swap in the new directory, so we have to get rid of it.
				reset(getWorkingDir(), nextDir);
				
				// displayOpenFiles("End of merge");
				
				_mergeNeeded = false;
			} catch (IOException e) {
				// displayOpenFiles("Exception during merge");
				
				IoUtils.closeAllQuietly(memoryIterator, activeIterator);
				FileUtils.deleteQuietly(nextDir);
				
				throw new RuntimeException(Thread.currentThread().getName() + ": Exception while merging: " + e.getMessage(), e);
			}
		}
	}

	private void displayOpenFiles(String header) {
		List<String> openFiles = _lsof.logOpenFiles(_dataDir);
		LOGGER.debug(header + ": open files");
		for (String filepath : openFiles) {
			LOGGER.debug(filepath);
		}
	}

	private File getWorkingDir() {
		return new File(_dataDir, WORKING_SUBDIR_NAME);
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
		
		DrumKeyValue memKV = null;
		DrumKeyValue diskKV = null;
		DrumKeyValue nextKV = null;
		
		// What we use when we have to merge memory/disk entries.
		DrumKeyValue mergedKV = new DrumKeyValue();
		
		RandomAccessFile memPayloadRAF = memKVIter.getPayloadRAF();
		RandomAccessFile diskPayloadRAF = diskKVIter.getPayloadRAF();
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
					// All done
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
				MergeResult result = _merger.doMerge(diskKV.getValue(), memKV.getValue(), mergedKV.getValue());

				if (result == MergeResult.USE_FIRST) {
					nextKV = diskKV;
					nextPayloadRAF = diskPayloadRAF;
				} else if (result == MergeResult.USE_SECOND) {
					nextKV = memKV;
					nextPayloadRAF = memPayloadRAF;
				} else if (result == MergeResult.USE_MERGED) {
					nextKV = mergedKV;
					// We could use either payload file, so just pick the memory one.
					nextPayloadRAF = memPayloadRAF;
					nextKV.setPayloadOffset(memKV.getPayloadOffset());
				} else {
					throw new RuntimeException("Unknown merge result: " + result);
				}
				
				hasMem = false;
				hasDisk = false;
			}
			
			addEntry(nextKV, nextPayloadRAF, queue, nextActiveFile, nextArchivedFile);
		}
		
		return nextDir;		
	}
	
	/**
	 * Create a URL from the KV and payload data, then try to add it to the queue. Based on the
	 * result, put it into either the active or the archived data.
	 * 
	 * @param dkv
	 * @param payloadRAF
	 * @param queue
	 * @param activeFile
	 * @param archivedFile
	 * @throws IOException
	 */
	private void addEntry(DrumKeyValue dkv, RandomAccessFile payloadRAF,
			FetchQueue queue, DrumMapFile activeFile, DrumMapFile archivedFile) throws IOException {
		CrawlStateUrl url = CrawlStateUrl.fromKV(dkv, payloadRAF);
		UrlState urlState = queue.add(url);
		
		// Call to add() can change status of URL, so reload the value.
		url.getValue(dkv.getValue());

		switch (urlState) {
		case ACTIVE:
			RandomAccessFile activePayloadRAF = activeFile.getPayloadFile().getRandomAccessFile();
			dkv.setPayloadOffset(activePayloadRAF.getFilePointer());
			dkv.write(activeFile.getKeyValueFile());
			url.write(activePayloadRAF);
			break;

		case ARCHIVE:
			RandomAccessFile archivePayloadRAF = archivedFile.getPayloadFile().getRandomAccessFile();
			dkv.setPayloadOffset(archivePayloadRAF.getFilePointer());
			dkv.write(archivedFile.getKeyValueFile());
			url.write(archivePayloadRAF);
			break;
		}
	}

}
