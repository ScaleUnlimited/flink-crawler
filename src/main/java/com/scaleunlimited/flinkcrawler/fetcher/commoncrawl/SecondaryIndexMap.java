package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Map from (reversed) URL to information about the primary index file (and offset/length of segment
 * in that file) where the URL would have to be found.
 *
 */

//Future - just have SecondaryIndex record which includes URL, do binary search against it.
//FUTURE - do forward key compression of URL data, by storing offset/length into byte array for
//forward key, and residual?


public class SecondaryIndexMap {
	static final Logger LOGGER = LoggerFactory.getLogger(SecondaryIndexMap.class);

    private String[] _secondaryIndexUrls;
    private SecondaryIndex[] _secondaryIndex;

    public SecondaryIndexMap() {
    	// no-args constructor for deserialization
    }
	public SecondaryIndexMap(String[] secondaryIndexUrls, SecondaryIndex[] secondaryIndex) {
		_secondaryIndexUrls = secondaryIndexUrls;
		_secondaryIndex = secondaryIndex;
	}
	
	/**
	 * Find the secondary index using the key. Return -1 if it can't exist.
	 * 
	 * @param key "reversed" URL
	 * @return SecondaryIndex record that might contain the URL, or null
	 */
	public SecondaryIndex get(String key) {
		int index = Arrays.binarySearch(_secondaryIndexUrls, key);
		if (index < 0) {
			index = -(index + 1);

			if (index <= 0) {
				return null;
			}

			// And now we know that the actual index will be the one before the insertion point,
			// so back it off by one.
			index -= 1;
			
			// LOGGER.debug(String.format("'%s' should be in segment %d based on secondary value '%s'", key, index, _secondaryIndexUrls[index]));
			// LOGGER.debug(String.format("First url in segment %d is '%s'", index + 1, _secondaryIndexUrls[index + 1]));
		}
		
		// TODO if the index is > 0, then we have an issue where the previous segment
		// could also contain this entry. Worst case, we could also have multiple segments
		// that have the target URL as their first entry. So we really need to return a
		// list (or start/end) of segment ids.

		return _secondaryIndex[index];
	}
	
	public int size() {
		return _secondaryIndex.length;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(_secondaryIndexUrls.length);
		for (String url : _secondaryIndexUrls) {
			out.writeUTF(url);
		}
		
		for (SecondaryIndex index : _secondaryIndex) {
			index.write(out);
		}
	}
	
	public void read(DataInput in) throws IOException {
		int numEntries = in.readInt();
		_secondaryIndexUrls = new String[numEntries];
		_secondaryIndex = new SecondaryIndex[numEntries];
		
		for (int i = 0; i < numEntries; i++) {
			_secondaryIndexUrls[i] = in.readUTF();
		}
		
		for (int i = 0; i < numEntries; i++) {
			SecondaryIndex si = new SecondaryIndex();
			si.read(in);
			_secondaryIndex[i] = si;
		}
	}
	
	public static class Builder {
		
	    // Format is <reversed domain>)<path><space><timestamp>\t<filename>\t<offset>\t<length>\t<sequence id>
	    // 146,207,118,124)/interviewpicservlet?curpage=0&sort=picall 20170427202839       cdx-00000.gz    4750379 186507  26
	    private static final Pattern IDX_LINE_PATTERN = Pattern.compile("(.+?)[ ]+(\\d+)\t(.+?)\t(\\d+)\t(\\d+)\t(\\d+)");
	    
		private List<String> _urls;
		private List<SecondaryIndex> _entries;
		
		public Builder(int size) {
			_urls = new ArrayList<>(size);
			_entries = new ArrayList<>(size);
		}
		
		public void add(String line) throws IllegalArgumentException {
			// 0,124,148,146)/index.php 20170429211342 cdx-00000.gz 0 195191 1
			Matcher m = IDX_LINE_PATTERN.matcher(line);
			if (!m.matches()) {
				throw new IllegalArgumentException("Invalid .idx line: " + line);
			}
			
			String url = m.group(1);
			
			String filename = m.group(3);
			long offset = Long.parseLong(m.group(4));
			long length = Long.parseLong(m.group(5));
			int id = Integer.parseInt(m.group(6));
			SecondaryIndex entry = new SecondaryIndex(filename, offset, length, id);
			
			_urls.add(url);
			_entries.add(entry);
		}
		
		public SecondaryIndexMap build() {
			String[] urls = _urls.toArray(new String[_urls.size()]);
			SecondaryIndex[] entries = _entries.toArray(new SecondaryIndex[_entries.size()]);
			return new SecondaryIndexMap(urls, entries);
		}
	}

}
