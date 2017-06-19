package com.scaleunlimited.flinkcrawler.fetcher;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class WarcRecordReader {

	private static final int MAX_LINE_LENGTH = 8192;

	private DataInputStream _input;
	private int _bytesRead;
	private byte[] _buffer;

	public WarcRecordReader(DataInputStream in) {
		_input = in;
		_bytesRead = 0;
		_buffer = new byte[MAX_LINE_LENGTH];

	}
	
	public WarcRecord readNextRecord() throws IOException {

		String version = "";
		
		// first - find our WARC header
		while (true) {
			String line = readLineFromInputStream().trim();
			if (line.startsWith(WarcRecord.WARC_VERSION)) {
				version = line.substring(WarcRecord.WARC_VERSION.length());
				break;
			}
		}

		// read until we see contentLength then an empty line
		// (to handle malformed ClueWeb09 headers that have blank lines)
		// get the content length and set our retContent
		int contentLength = -1;
		List<String> warcHeaders = new ArrayList<>();
		List<String> httpHeaders = new ArrayList<>();
		
		while (true) {
			String line = readLineFromInputStream().trim();
			if (line.length() > 0) {
				warcHeaders.add(line);
				if (line.startsWith(WarcRecord.WARC_CONTENTLENGTH_FIELDNAME)) {
					contentLength = Integer.parseInt(line.substring(WarcRecord.WARC_CONTENTLENGTH_FIELDNAME.length()).trim());
				}
			} else {
				break;
			}
		}
		
		if (contentLength == -1) {
			throw new IOException("No Content-Length header found");
		}
		
		// Assume we have HTTP(S) response headers.
		int httpHeadersLength = 0;
		while (true) {
			byte[] lineData = readBytesFromInputStream();
			// We've just consumed this much data, plus the CRLF at end of the line.
			httpHeadersLength += lineData.length + 2;
			
			if (lineData.length > 0) {
				httpHeaders.add(new String(lineData, StandardCharsets.UTF_8));
			} else {
				break;
			}
		}
		
		// Now we need to read in the content bytes
		contentLength -= httpHeadersLength;
		byte[] content = new byte[contentLength];
		if (IOUtils.read(_input, content) != contentLength) {
			throw new IOException("Not enough bytes for requested content length");
		}
		
		WarcRecord result = new WarcRecord(version);
		result.setHttpHeaders(httpHeaders);
		
		for (String warcHeader : warcHeaders) {
			String[] pieces = warcHeader.split(":", 2);
			if (pieces.length != 2) {
				result.addHeaderMetadata(pieces[0], "");
			} else {
				String thisKey = pieces[0].trim();
				String thisValue = pieces[1].trim();
				result.addHeaderMetadata(thisKey, thisValue);
			}
		}

		// set the content
		result.setContent(content);

		return result;
	}

	public int getBytesRead() {
		return _bytesRead;
	}
	
	private String readLineFromInputStream() throws IOException {
		return new String(readBytesFromInputStream(), StandardCharsets.UTF_8);
	}

	private byte[] readBytesFromInputStream() throws IOException {
		int length = 0;
		boolean hasCR = false;
		
		while (length < _buffer.length) {
			byte curByte = _input.readByte();
			_bytesRead += 1;
			
			if (curByte == WarcRecord.CR_BYTE) {
				if (hasCR) {
					throw new IOException("Invalid byte sequence, got CR+CR sequence");
				} else {
					hasCR = true;
				}
			} else if (curByte == WarcRecord.LF_BYTE) {
				if (!hasCR) {
					throw new IOException("Invalid byte sequence, got LF without CR");
				} else {
					byte[] result = new byte[length];
					System.arraycopy(_buffer, 0, result, 0, length);
					return result;
				}
			} else if (hasCR) {
				throw new IOException("Invalid byte sequence, got CR without following LF");
			} else {
				_buffer[length++] = curByte;
			}
		}
		
		throw new IOException("No CRLF found in lots of data");
	}


}
