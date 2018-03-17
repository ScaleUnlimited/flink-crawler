package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SecondaryIndex {
	
    private static final Pattern FILENAME_PATTERN = Pattern.compile("cdx-(\\d{5}).gz");
    
    // WARNING - if anything changes in the serialization of this class, you must
    // (a) bump the version number in SecondaryIndexMap, and (b) decide whether
    // you'll support deserializing the older format.
    
    private int _indexFilenumber;
	
	private long _segmentOffset;
	private long _segmentLength;
	private int _segmentId;
	
	public SecondaryIndex() {
		// no-args constructor for deserialization
	}
	
	public SecondaryIndex(String indexFilename, long segmentOffset, long segmentLength, int segmentId) {
	    Matcher m = FILENAME_PATTERN.matcher(indexFilename);
	    if (!m.matches()) {
	        throw new IllegalArgumentException("Invalid index filename: " + indexFilename);
	    }
	    
	    _indexFilenumber = Integer.parseInt(m.group(1));
		_segmentOffset = segmentOffset;
		_segmentLength = segmentLength;
		_segmentId = segmentId;
	}

	public String getIndexFilename() {
		return String.format("cdx-%05d.gz", _indexFilenumber);
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
		out.writeInt(_indexFilenumber);
		out.writeLong(_segmentOffset);
		out.writeLong(_segmentLength);
		out.writeInt(_segmentId);
	}
	
	public void read(DataInput in) throws IOException {
	    _indexFilenumber = in.readInt();
		_segmentOffset = in.readLong();
		_segmentLength = in.readLong();
		_segmentId = in.readInt();
		
	}
}
