package com.scaleunlimited.flinkcrawler.warc;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Based on code from https://github.com/ept/warc-hadoop 
 * (c) 2014 Martin Kleppmann. MIT License.
 * 
 * Writes {@link WARCRecord}s to a WARC file, using Hadoop's filesystem APIs. (This means you can
 * write to HDFS, S3 or any other filesystem supported by Hadoop). This implementation is not tied
 * to the MapReduce APIs -- that link is provided by the mapred
 * {@link com.martinkl.warc.mapred.WARCOutputFormat} and the mapreduce
 * {@link com.martinkl.warc.mapreduce.WARCOutputFormat}.
 *
 * WARCFileWriter keeps track of how much data it has written (optionally gzip-compressed); when the
 * file becomes larger than some threshold, it is automatically closed and a new segment is started.
 * A segment number is appended to the filename for that purpose. The segment number always starts
 * at 00000, and by default a new segment is started when the file size exceeds 1GB. To change the
 * target size for a segment, you can set the `warc.output.segment.size` key in the Hadoop
 * configuration to the number of bytes. (Files may actually be a bit larger than this threshold,
 * since we finish writing the current record before opening a new file.)
 */
public class WARCFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(WARCFileWriter.class);
    public static final long DEFAULT_MAX_SEGMENT_SIZE = 1_000_000_000L; // 1 GB

    private final Configuration _conf;
    private final CompressionCodec _codec;
    private final Path _workOutputPath;
    private final Progressable _progress;
    private final String _extensionFormat;
    private final long _maxSegmentSize;
    private long _segmentsCreated = 0, _segmentsAttempted = 0, _bytesWritten = 0;
    private CountingOutputStream _byteStream;
    private DataOutputStream _dataStream;

    /**
     * Creates a WARC file, and opens it for writing. If a file with the same name already exists,
     * an attempt number in the filename is incremented until we find a file that doesn't already
     * exist.
     *
     * @param conf
     *            The Hadoop configuration.
     * @param codec
     *            If null, the file is uncompressed. If non-null, this compression codec will be
     *            used. The codec's default file extension is appended to the filename.
     * @param workOutputPath
     *            The directory and filename prefix to which the data should be written. We append a
     *            segment number and filename extensions to it.
     * @throws IOException
     */
    public WARCFileWriter(Configuration conf, CompressionCodec codec, Path workOutputPath)
            throws IOException {
        this(conf, codec, workOutputPath, null);
    }

    /**
     * Creates a WARC file, and opens it for writing. If a file with the same name already exists,
     * it is *overwritten*. Note that this is different behaviour from the other constructor. Yes,
     * this sucks. It will probably change in a future version.
     *
     * @param conf
     *            The Hadoop configuration.
     * @param codec
     *            If null, the file is uncompressed. If non-null, this compression codec will be
     *            used. The codec's default file extension is appended to the filename.
     * @param workOutputPath
     *            The directory and filename prefix to which the data should be written. We append a
     *            segment number and filename extensions to it.
     * @param progress
     *            An object used by the mapred API for tracking a task's progress.
     * @throws IOException
     */
    public WARCFileWriter(Configuration conf, CompressionCodec codec, Path workOutputPath,
            Progressable progress) throws IOException {
        this._conf = conf;
        this._codec = codec;
        this._workOutputPath = workOutputPath;
        this._progress = progress;
        this._extensionFormat = ".seg-%05d.attempt-%05d.warc"
                + (codec == null ? "" : codec.getDefaultExtension());
        this._maxSegmentSize = conf.getLong("warc.output.segment.size", DEFAULT_MAX_SEGMENT_SIZE);
        createSegment();
    }

    /**
     * Instantiates a Hadoop codec for compressing and decompressing Gzip files. This is the most
     * common compression applied to WARC files.
     *
     * @param conf
     *            The Hadoop configuration.
     */
    public static CompressionCodec getGzipCodec(Configuration conf) {
        try {
            return (CompressionCodec) ReflectionUtils
                    .newInstance(conf.getClassByName("org.apache.hadoop.io.compress.GzipCodec")
                            .asSubclass(CompressionCodec.class), conf);
        } catch (ClassNotFoundException e) {
            logger.warn("GzipCodec could not be instantiated", e);
            return null;
        }
    }

    /**
     * Creates an output segment file and sets up the output streams to point at it. If the file
     * already exists, retries with a different filename. This is a bit nasty -- after all,
     * {@link FileOutputFormat}'s work directory concept is supposed to prevent filename clashes --
     * but it looks like Amazon Elastic MapReduce prevents use of per-task work directories if the
     * output of a job is on S3.
     *
     * TODO: Investigate this and find a better solution.
     */
    private void createSegment() throws IOException {
        _segmentsAttempted = 0;
        _bytesWritten = 0;
        boolean success = false;

        while (!success) {
            Path path = _workOutputPath
                    .suffix(String.format(_extensionFormat, _segmentsCreated, _segmentsAttempted));
            FileSystem fs = path.getFileSystem(_conf);

            try {
                // The o.a.h.mapred OutputFormats overwrite existing files, whereas
                // the o.a.h.mapreduce OutputFormats don't overwrite. Bizarre...
                // Here, overwrite if progress != null, i.e. if using mapred API.
                FSDataOutputStream fsStream = (_progress == null) ? fs.create(path, false)
                        : fs.create(path, _progress);
                _byteStream = new CountingOutputStream(new BufferedOutputStream(fsStream));
                _dataStream = new DataOutputStream(
                        _codec == null ? _byteStream : _codec.createOutputStream(_byteStream));
                _segmentsCreated++;
                logger.info("Writing to output file: {}", path);
                success = true;

            } catch (IOException e) {
                if (e.getMessage().startsWith("File already exists")) {
                    logger.warn("Tried to create file {} but it already exists; retrying.", path);
                    _segmentsAttempted++; // retry
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Appends a {@link WARCRecord} to the file, in WARC/1.0 format.
     * 
     * @param record
     *            The record to be written.
     * @throws IOException
     */
    public void write(WARCRecord record) throws IOException {
        if (_bytesWritten > _maxSegmentSize) {
            _dataStream.close();
            createSegment();
        }
        record.write(_dataStream);
    }

    /**
     * Appends a {@link WARCRecord} wrapped in a {@link WARCWritable} to the file.
     * 
     * @param record
     *            The wrapper around the record to be written.
     * @throws IOException
     */
    public void write(WARCWritable record) throws IOException {
        if (record.getRecord() != null)
            write(record.getRecord());
    }

    /**
     * Flushes any buffered data and closes the file.
     * 
     * @throws IOException
     */
    public void close() throws IOException {
        _dataStream.close();
    }

    private class CountingOutputStream extends FilterOutputStream {
        public CountingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            _bytesWritten += len;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            _bytesWritten++;
        }

        // Overriding close() because FilterOutputStream's close() method pre-JDK8 has bad behavior:
        // it silently ignores any exception thrown by flush(). Instead, just close the delegate
        // stream.
        // It should flush itself if necessary. (Thanks to the Guava project for noticing this.)
        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}