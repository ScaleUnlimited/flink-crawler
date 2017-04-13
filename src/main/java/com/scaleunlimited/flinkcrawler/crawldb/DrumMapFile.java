package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.scaleunlimited.flinkcrawler.utils.IoUtils;

public class DrumMapFile implements Closeable {

	private DrumKeyValueFile _keyValueFile;
	private DrumPayloadFile _payloadFile;
	
	public DrumMapFile(File dir, String prefix, boolean delete) throws IOException {
		_keyValueFile = new DrumKeyValueFile(createFile(dir, prefix, DrumMapFileType.KEY_VALUE, delete), delete ? "rw" : "r");
		_payloadFile = new DrumPayloadFile(createFile(dir, prefix, DrumMapFileType.PAYLOAD, delete));
	}

	private File createFile(File dir, String prefix, DrumMapFileType fileType, boolean delete) throws IOException {
		File result = new File(dir, makeFilename(prefix, fileType));
		if (result.exists() && delete) {
			result.delete();
		}
		
		result.createNewFile();
		return result;
	}
	
	@Override
	public void close() throws IOException {
		IoUtils.closeAll(_keyValueFile, _payloadFile);
		_keyValueFile = null;
		_payloadFile = null;
	}
	
	public static String makeFilename(String prefix, DrumMapFileType fileType) {
		return String.format("%s-%s.bin", prefix, fileType);
	}
	
	public enum DrumMapFileType {
		KEY_VALUE("keyvalue"),
		PAYLOAD("payload");
		
		private String _filenamePrefix;
		
		private DrumMapFileType(String filenamePrefix) {
			_filenamePrefix = filenamePrefix;
		}
		
		public String getFilenamePrefix() {
			return _filenamePrefix;
		}
	}

	public DrumPayloadFile getPayloadFile() {
		return _payloadFile;
	}

	public DrumKeyValueFile getKeyValueFile() {
		return _keyValueFile;
	}

	public boolean isEmpty() throws IOException {
		return _keyValueFile.length() == 0L;
	}
}
