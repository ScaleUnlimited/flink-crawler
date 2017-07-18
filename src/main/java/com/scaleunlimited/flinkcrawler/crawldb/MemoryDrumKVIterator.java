package com.scaleunlimited.flinkcrawler.crawldb;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger.MergeResult;
import com.scaleunlimited.flinkcrawler.utils.ByteUtils;

public class MemoryDrumKVIterator extends DrumKVIterator {

	private BaseCrawlDBMerger _merger;
	private int _numEntries;
	private int[] _entries;
	private byte[] _entryData;
	
	private int _curEntry;
	private byte[] _mergedValue;
	private byte[] _nextValue;
	
	public MemoryDrumKVIterator(BaseCrawlDBMerger merger, int numEntries, int[] entries, byte[] entryData, DrumPayloadFile payloadFile) {
		super(payloadFile);
		
		_merger = merger;
		_numEntries = numEntries;
		_entries = entries;
		_entryData = entryData;
		
		_mergedValue = new byte[1 + DrumKeyValue.MAX_VALUE_LENGTH];
		_nextValue = new byte[1 + DrumKeyValue.MAX_VALUE_LENGTH];
	}

	@Override
	public boolean hasNext() {
		return _curEntry < _numEntries;
	}

	@Override
	public DrumKeyValue next() {
		return next(null);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove is not supported");
	}

	@Override
	public DrumKeyValue next(DrumKeyValue result) {
		if (!hasNext()) {
			throw new IllegalStateException("No next result to return");
		}
		
		if (result == null) {
			result = new DrumKeyValue();
		}
		
		int curOffset = _entries[_curEntry++];
		result.setKeyHash(ByteUtils.bytesToLong(_entryData, curOffset));
		curOffset += 8;
		result.setPayloadOffset(ByteUtils.bytesToLong(_entryData, curOffset));
		curOffset += 8;
		int valueLength = ByteUtils.bytesToUnsignedByte(_entryData, curOffset);
		byte[] value = result.getValue();
		value[0] = (byte)valueLength;
		
		curOffset += 1;
		System.arraycopy(_entryData, curOffset, value, 1, valueLength);
		
		// Now see if the next entry is a duplicate of this one. If so, we need to
		// merge the results.
		while (hasNext() && (result.getKeyHash() == getKeyHash())) {
			getValue(_nextValue);
			MergeResult mr = _merger.doMerge(result.getValue(), _nextValue, _mergedValue);
			if (mr == MergeResult.USE_OLD) {
				// all set
			} else if  (mr == MergeResult.USE_NEW) {
				result.setValue(_nextValue);
			} else if (mr == MergeResult.USE_MERGED) {
				result.setValue(_mergedValue);
			} else {
				throw new RuntimeException("Unknown merge result: " + mr);
			}
			
			// Advance past this entry.
			_curEntry += 1;
		}
		
		return result;
	}
	
	private void getValue(byte[] value) {
		int curOffset = _entries[_curEntry];
		curOffset += 16;
		int valueLength = (byte)_entryData[curOffset++];
		value[0] = (byte)valueLength;
		System.arraycopy(_entryData, curOffset, value, 1, valueLength);
	}
	
	private long getKeyHash() {
		int curOffset = _entries[_curEntry];
		return ByteUtils.bytesToLong(_entryData, curOffset);
	}

}
