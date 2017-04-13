package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DrumKeyValueFile extends RandomAccessFile {

	public DrumKeyValueFile(String name, String mode) throws FileNotFoundException {
		super(name, mode);
	}

	public DrumKeyValueFile(File file, String mode) throws FileNotFoundException {
		super(file, mode);
	}

	public int read(DrumKeyValue keyValue) throws IOException {
		keyValue.setKeyHash(readLong());
		keyValue.setPayloadOffset(readLong());

		int valueLength = readUnsignedByte();
		byte[] value = keyValue.getValue();
		value[0] = (byte)valueLength;
		read(value, 1, valueLength);
		
		// Two longs, a value length byte, and the length of the value.
		return 8 + 8 + 1 + valueLength;
	}

	public void write(DrumKeyValue dkv) throws IOException {
		writeLong(dkv.getKeyHash());
		writeLong(dkv.getPayloadOffset());
		
		int valueLength = dkv.getValueLength();
		writeByte(valueLength);
		write(dkv.getValue(), 1, valueLength);
	}
}
