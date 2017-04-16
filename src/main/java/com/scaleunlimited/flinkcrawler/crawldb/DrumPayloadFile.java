package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Closeable;
import java.io.DataInput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.io.FileUtils;

public class DrumPayloadFile implements Closeable {

	private File _payloadFile;
	
	private transient DrumDataOutput _dataOutput;
	private transient RandomAccessFile _randomAccess;
	
	public DrumPayloadFile(File f) {
		_payloadFile = f;
	}
	
	public void create() throws IOException {
		_payloadFile.delete();
		_payloadFile.createNewFile();
	}
	
	public void createIfMissing() throws IOException {
		if (!_payloadFile.exists()) {
			create();
		}
		
		// TODO verify it's a file that we can read, in the right format.
	}

	public DrumDataOutput getDrumDataOutput() throws IOException {
		if (_randomAccess != null) {
			_randomAccess.close();
			_randomAccess = null;
		}
		
		if (_dataOutput == null) {
			_dataOutput = new DrumDataOutput(new FileOutputStream(_payloadFile));
		}
		
		return _dataOutput;
	}
	
	public RandomAccessFile getRandomAccessFile() throws IOException {
		if (_dataOutput != null) {
			_dataOutput.close();
			_dataOutput = null;
		}
		
		if (_randomAccess == null) {
			_randomAccess = new RandomAccessFile(_payloadFile, isEmpty() ? "rw" : "r");
		}
		
		return _randomAccess;
	}

	@Override
	public void close() throws IOException {
		if (_dataOutput != null) {
			_dataOutput.close();
			_dataOutput = null;
		}
		
		if (_randomAccess != null) {
			_randomAccess.close();
			_randomAccess = null;
		}
	}

	public DataInput getInputStream(long pos) throws IOException {
		RandomAccessFile result = getRandomAccessFile();
		result.seek(pos);
		return result;
	}
	
	public long getBytesWritten() throws IOException {
		return getDrumDataOutput().getBytesWritten();
	}

	public boolean isEmpty() {
		return FileUtils.sizeOf(_payloadFile) == 0;
	}

}
