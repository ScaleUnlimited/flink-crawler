package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;

public class DrumKeyValue {

	private long _keyHash;
	private long _payloadOffset;
	private byte[] _value;
	
	public DrumKeyValue() {
		_value = new byte[1 + DrumMap.MAX_VALUE_LENGTH];
	}

	public long getKeyHash() {
		return _keyHash;
	}

	public void setKeyHash(long keyHash) {
		_keyHash = keyHash;
	}

	public long getPayloadOffset() {
		return _payloadOffset;
	}

	public void setPayloadOffset(long payloadOffset) {
		_payloadOffset = payloadOffset;
	}

	public int getValueLength() {
		return _value[0] & 0x0FF;
	}

	public byte[] getValue() {
		return _value;
	}

	public void setValue(byte[] value) {
		int valueLength = _value[0] & 0x0FF;
		System.arraycopy(value, 0, _value, 0, valueLength + 1);
	}

	public long readFields(DataInput in) throws IOException {
		setKeyHash(in.readLong());
		long result = 8;

		setPayloadOffset(in.readLong());
		result += 8;

		int valueLength = in.readUnsignedByte();
		_value[0] = (byte)valueLength;
		result += 1;

		in.readFully(_value, 1, valueLength);
		result += valueLength;
		return result;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(_keyHash);
		out.writeLong(_payloadOffset);
		int valueLength = _value[0] & 0x0FF;
		out.writeByte(valueLength);
		out.write(_value, 1, valueLength);
	}

}
