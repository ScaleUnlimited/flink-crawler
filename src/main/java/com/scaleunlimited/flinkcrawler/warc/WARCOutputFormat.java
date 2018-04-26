package com.scaleunlimited.flinkcrawler.warc;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hadoop OutputFormat for mapreduce jobs ('new' API) that want to write data to WARC files.
 *
 * Usage:
 *
 * ```java Job job = new Job(getConf()); job.setOutputFormatClass(WARCOutputFormat.class);
 * job.setOutputKeyClass(NullWritable.class); job.setOutputValueClass(WARCWritable.class);
 * FileOutputFormat.setCompressOutput(job, true); ```
 *
 * The tasks generating the output (usually the reducers, but may be the mappers if there are no
 * reducers) should use `NullWritable.get()` as the output key, and the {@link WARCWritable} as the
 * output value.
 */
public class WARCOutputFormat extends FileOutputFormat<NullWritable, WARCWritable> {

    /**
     * Creates a new output file in WARC format, and returns a RecordWriter for writing to it.
     */
    @Override
    public RecordWriter<NullWritable, WARCWritable> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new WARCWriter(context);
    }

    private class WARCWriter extends RecordWriter<NullWritable, WARCWritable> {
        private final WARCFileWriter writer;

        public WARCWriter(TaskAttemptContext context) throws IOException {
            Configuration conf = context.getConfiguration();
            CompressionCodec codec = getCompressOutput(context) ? WARCFileWriter.getGzipCodec(conf)
                    : null;
            Path workFile = getDefaultWorkFile(context, "");
            this.writer = new WARCFileWriter(conf, codec, workFile);
        }

        @Override
        public void write(NullWritable key, WARCWritable value)
                throws IOException, InterruptedException {
            writer.write(value);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            writer.close();
        }
    }
}