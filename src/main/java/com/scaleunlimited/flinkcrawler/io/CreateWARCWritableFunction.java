package com.scaleunlimited.flinkcrawler.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.NullWritable;

import com.scaleunlimited.flinkcrawler.functions.BaseFlatMapFunction;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;

@SuppressWarnings("serial")
public class CreateWARCWritableFunction extends BaseFlatMapFunction<FetchResultUrl, Tuple2<NullWritable, WARCWritable>> {

    SimpleDateFormat _dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    
    private static String DEFAULT_DESCRIPTION = "URL extracted using flink-crawler";
    private String _description = DEFAULT_DESCRIPTION;
    private String _publisher;
    
    public void setDescription(String description) {
        _description = description;
    }
    public void setPublisher(String publisher) {
        _publisher = publisher;
    }
    @Override
    public void flatMap(FetchResultUrl fetchResultUrl, Collector<Tuple2<NullWritable, WARCWritable>> collector)
            throws Exception {
        
        // TODO - fill in the actual warc info here
        
        // TODO - what we want is the warcinfo written out once at the start and then we want to follow that with
        // a resource record.
        
        StringBuffer content = new StringBuffer();

        content.append(String.format("url: %s\r\n", fetchResultUrl.getUrl())); // TODO verify if this field is correct
        content.append(String.format("hostname: %s\r\n", fetchResultUrl.getHostname()));
        content.append(String.format("description: %s\r\n", _description));
        if (_publisher != null) {
            content.append(String.format("publisher: %s\r\n", _publisher));
        }
        content.append("format: WARC File Format 1.0\r\n");
        content.append("conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf\r\n");
        // TODO add the actual fetched content ?
        
        StringBuffer buffer = new StringBuffer();
        buffer.append("WARC/1.0\r\n");
        buffer.append("WARC-Type: warcinfo\r\n");
        buffer.append(String.format("WARC-Date: %s\r\n", _dateFormat.format(new Date())));
        buffer.append("WARC-Record-ID: <urn:uuid:d9bbb325-c09f-473c-8600-1c9dbd4ec443>\r\n"); // TODO create uuid ?
        buffer.append(String.format("Content-Length: %d\r\n", content.length()));
        buffer.append("Content-Type: application/warc-fields\r\n");
        buffer.append("WARC-Filename: CC-MAIN-20140313024455-00000-ip-10-183-142-35.ec2.internal.warc.gz\r\n"); // TODO where do we get the filename ?
        buffer.append("\r\n");
        buffer.append(content);
        buffer.append("\r\n");
        buffer.append("\r\n");
        buffer.append("\r\n");
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(buffer.toString().getBytes("UTF-8")));
        WARCRecord record = new WARCRecord(stream);
        WARCWritable writable = new WARCWritable(record);
        collector.collect(new Tuple2<NullWritable, WARCWritable>(NullWritable.get(), writable));
    }

}
