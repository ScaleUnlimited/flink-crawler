package com.scaleunlimited.flinkcrawler.warc;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * A mutable wrapper around a {@link WARCRecord} implementing the Hadoop Writable interface. This
 * allows WARC records to be used throughout Hadoop (e.g. written to sequence files when shuffling
 * data between mappers and reducers). The record is encoded as a single record in standard WARC/1.0
 * format.
 */
public class WARCWritable implements IOReadableWritable {

    private WARCRecord record;

    /** Creates an empty writable (with a null record). */
    public WARCWritable() {
        this.record = null;
    }

    /** Creates a writable wrapper around a given WARCRecord. */
    public WARCWritable(WARCRecord record) {
        this.record = record;
    }

    /** Returns the record currently wrapped by this writable. */
    public WARCRecord getRecord() {
        return record;
    }

    /** Updates the record held within this writable wrapper. */
    public void setRecord(WARCRecord record) {
        this.record = record;
    }

    /**
     * Parses a {@link WARCRecord} out of a {@link DataInputView}, and makes it the writable's
     * current record.
     */
    @Override
    public void read(DataInputView in) throws IOException {
        record = new WARCRecord(in);
    }

    /** Appends the current record to a {@link DataOutputView}. */
    @Override
    public void write(DataOutputView out) throws IOException {
        // TODO Auto-generated method stub
        if (record != null) {
            record.write(out);
        }

    }
}