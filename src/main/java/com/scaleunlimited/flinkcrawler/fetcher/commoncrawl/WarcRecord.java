package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * Revised version of WARC processing code, originally forked from Lemur project.
 * 
 * See https://github.com/iipc/warc-specifications/blob/gh-pages/specifications/warc-format/warc-1.1/index.md for the
 * actual WARC spec.
 */
public class WarcRecord {

    protected static final String WARC_VERSION = "WARC/";
    protected static final String WARC_CONTENTLENGTH_FIELDNAME = "Content-Length:";

    protected static final byte CR_BYTE = 13;
    protected static final byte LF_BYTE = 10;

    private static final String LINE_ENDING = new String(new byte[] {
            CR_BYTE, LF_BYTE
    }, StandardCharsets.UTF_8);

    public class WarcHeader {
        public String version = "";
        public String contentType = "";
        public String UUID = "";
        public String dateString = "";
        public String recordType = "";
        public HashMap<String, String> metadata = new HashMap<String, String>();
        public int contentLength = 0;

        public WarcHeader(String version) {
            this.version = version;
        }

        @Override
        public String toString() {
            StringBuffer retBuffer = new StringBuffer();

            retBuffer.append(WARC_VERSION);
            retBuffer.append(version);
            retBuffer.append(LINE_ENDING);

            retBuffer.append("WARC-Type: " + recordType + LINE_ENDING);
            retBuffer.append("WARC-Date: " + dateString + LINE_ENDING);

            Iterator<Entry<String, String>> metadataIterator = metadata.entrySet().iterator();
            while (metadataIterator.hasNext()) {
                Entry<String, String> thisEntry = metadataIterator.next();
                retBuffer.append(thisEntry.getKey());
                retBuffer.append(": ");
                retBuffer.append(thisEntry.getValue());
                retBuffer.append(LINE_ENDING);
            }

            // Keep this as the last WARC-...
            retBuffer.append("WARC-Record-ID: " + UUID + LINE_ENDING);

            retBuffer.append("Content-Type: " + contentType + LINE_ENDING);
            retBuffer.append("Content-Length: " + contentLength + LINE_ENDING);

            return retBuffer.toString();
        }
    }

    private WarcHeader warcHeader;
    private List<String> httpHeaders;
    private byte[] warcContent = null;
    private String warcFilePath = "";

    public WarcRecord(String version) {
        warcHeader = new WarcHeader(version);
        httpHeaders = new ArrayList<>();
    }

    public void setHttpHeaders(List<String> httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

    public List<String> getHttpHeaders() {
        return httpHeaders;
    }

    public String getHttpHeader(String key) {
        for (String httpHeader : httpHeaders) {
            String[] keyValue = httpHeader.split(":", 2);
            if (keyValue[0].equalsIgnoreCase(key)) {
                return keyValue[1].trim();
            }
        }

        return null;
    }

    public void setWarcFilePath(String path) {
        warcFilePath = path;
    }

    public String getWarcFilePath() {
        return warcFilePath;
    }

    public void setWarcRecordType(String recordType) {
        warcHeader.recordType = recordType;
    }

    public void setWarcContentType(String contentType) {
        warcHeader.contentType = contentType;
    }

    public void setWarcDate(String dateString) {
        warcHeader.dateString = dateString;
    }

    public void setWarcUUID(String UUID) {
        warcHeader.UUID = UUID;
    }

    public void setWarcContentLength(int len) {
        warcHeader.contentLength = len;
    }

    public void addHeaderMetadata(String key, String value) {
        // System.out.println("+-- WarRecord.addHeaderMetadata key=" + key + "
        // value=" + value);
        // don't allow addition of known keys
        if (key.equals("WARC-Type")) {
            setWarcRecordType(value);
        } else if (key.equals("WARC-Date")) {
            setWarcDate(value);
        } else if (key.equals("WARC-Record-ID")) {
            setWarcUUID(value);
        } else if (key.equals("Content-Type")) {
            setWarcContentType(value);
        } else {
            warcHeader.metadata.put(key, value);
        }
    }

    public void addHeaderMetadata(String key, int value) {
        if (key.equals("Content-Length")) {
            setWarcContentLength(value);
        } else {
            warcHeader.metadata.put(key, "" + value);
        }
    }

    public void clearHeaderMetadata() {
        warcHeader.metadata.clear();
    }

    public Set<Entry<String, String>> getHeaderMetadata() {
        return warcHeader.metadata.entrySet();
    }

    public String getHeaderMetadataItem(String key) {
        if (key.equals("WARC-Type")) {
            return warcHeader.recordType;
        } else if (key.equals("WARC-Date")) {
            return warcHeader.dateString;
        } else if (key.equals("WARC-Record-ID")) {
            return warcHeader.UUID;
        } else if (key.equals("Content-Type")) {
            return warcHeader.contentType;
        } else if (key.equals("Content-Length")) {
            return Integer.toString(warcHeader.contentLength);
        } else {
            return warcHeader.metadata.get(key);
        }
    }

    public void setContent(byte[] content) {
        warcContent = content;
        warcHeader.contentLength = content.length;
    }

    public void setContent(String content) {
        setContent(content.getBytes());
    }

    public byte[] getContent() {
        return warcContent;
    }

    public byte[] getByteContent() {
        return warcContent;
    }

    public String getContentUTF8() {
        return new String(warcContent, StandardCharsets.UTF_8);
    }

    public String getHeaderRecordType() {
        return warcHeader.recordType;
    }

    @Override
    public String toString() {
        StringBuffer retBuffer = new StringBuffer();
        retBuffer.append(warcHeader.toString());
        retBuffer.append(LINE_ENDING);
        retBuffer.append(getContentUTF8());
        return retBuffer.toString();
    }

    public String getHeaderString() {
        return warcHeader.toString();
    }

}
