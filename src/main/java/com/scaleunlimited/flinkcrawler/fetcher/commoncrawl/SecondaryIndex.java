package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondaryIndex {
	
	private String _indexFilename;
	
	private long _segmentOffset;
	private long _segmentLength;
	private int _segmentId;
	
	public SecondaryIndex() {
		// no-args constructor for deserialization
	}
	
	public SecondaryIndex(String indexFilename, long segmentOffset, long segmentLength, int segmentId) {
		_indexFilename = indexFilename;
		_segmentOffset = segmentOffset;
		_segmentLength = segmentLength;
		_segmentId = segmentId;
	}

	public String getIndexFilename() {
		return _indexFilename;
	}

	public long getSegmentOffset() {
		return _segmentOffset;
	}

	public long getSegmentLength() {
		return _segmentLength;
	}

	public int getSegmentId() {
		return _segmentId;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(_indexFilename);
		out.writeLong(_segmentOffset);
		out.writeLong(_segmentLength);
		out.writeInt(_segmentId);
	}
	
	public void read(DataInput in) throws IOException {
		_indexFilename = in.readUTF();
		_segmentOffset = in.readLong();
		_segmentLength = in.readLong();
		_segmentId = in.readInt();
		
	}
}
