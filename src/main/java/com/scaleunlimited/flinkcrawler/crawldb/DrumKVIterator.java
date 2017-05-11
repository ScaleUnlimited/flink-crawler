package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

public abstract class DrumKVIterator implements Iterator<DrumKeyValue>, Closeable {

	private DrumPayloadFile _payloadFile;
	
	public DrumKVIterator(DrumPayloadFile payloadFile) {
		_payloadFile = payloadFile;
	}
	
	public RandomAccessFile getPayloadRAF() throws IOException {
		return _payloadFile.getRandomAccessFile();
	}
	
	// More memory-efficient version of next that can re-use a value
	// (pass null to allocate the same as next()).
	public abstract DrumKeyValue next(DrumKeyValue result);
	
	// Return the payload from the DrumKeyValue
	public void getPayload(DrumKeyValue kv, IPayload result) throws IOException {
		DataInput di = _payloadFile.getInputStream(kv.getPayloadOffset());
		result.readFields(di);
	}
	
	@Override
	public void close() throws IOException {
		_payloadFile.close();
	}
}
