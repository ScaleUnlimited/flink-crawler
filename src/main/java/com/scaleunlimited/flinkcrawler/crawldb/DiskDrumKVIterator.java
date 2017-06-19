package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.IOException;
import java.io.RandomAccessFile;

public class DiskDrumKVIterator extends DrumKVIterator {

	private RandomAccessFile _kvFile;

	private long _kvFileLength;
	private long _curOffset;
	
	public DiskDrumKVIterator(DrumMapFile file) throws IOException {
		super(file.getPayloadFile());
		
		_kvFile = file.getKeyValueFile();
		_kvFileLength = _kvFile.length();
		_curOffset = 0;
	}

	@Override
	public void close() throws IOException {
		_kvFile.close();
		super.close();
	}
	
	@Override
	public boolean hasNext() {
		return _curOffset < _kvFileLength;
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
		if (result == null) {
			result = new DrumKeyValue();
		}
		
		try {
			_curOffset += result.readFields(_kvFile);
		} catch (IOException e) {
			throw new RuntimeException("Error reading next KV", e);
		}
		
		return result;
	}
	

}
