package com.scaleunlimited.flinkcrawler.warc;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Based on code from https://github.com/ept/warc-hadoop 
 * (c) 2014 Martin Kleppmann. MIT License.
 * 
 * A mutable wrapper around a {@link WARCRecord} implementing the Flink IOReadableWritable interface. This
 * allows WARC records to be used throughout Flink (e.g. written to sequence files when shuffling
 * data between mappers and reducers). The record is encoded as a single record in standard WARC/1.0
 * format.
 */
public class WARCWritable implements IOReadableWritable {

    private WARCRecord _record;

    /** Creates an empty writable (with a null record). */
    public WARCWritable() {
        _record = null;
    }

    /** Creates a writable wrapper around a given WARCRecord. */
    public WARCWritable(WARCRecord record) {
        _record = record;
    }

    /** Returns the record currently wrapped by this writable. */
    public WARCRecord getRecord() {
        return _record;
    }

    /** Updates the record held within this writable wrapper. */
    public void setRecord(WARCRecord record) {
        _record = record;
    }

    /**
     * Parses a {@link WARCRecord} out of a {@link DataInputView}, and makes it the writable's
     * current record.
     */
    @Override
    public void read(DataInputView in) throws IOException {
        _record = new WARCRecord(in);
    }

    /** Appends the current record to a {@link DataOutputView}. */
    @Override
    public void write(DataOutputView out) throws IOException {
        if (_record != null) {
            _record.write(out);
        }

    }
}