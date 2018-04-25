package com.scaleunlimited.flinkcrawler.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.NullWritable;

import com.scaleunlimited.flinkcrawler.functions.BaseFlatMapFunction;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;

@SuppressWarnings("serial")
public class CreateWARCWritableFunction extends BaseFlatMapFunction<FetchResultUrl, Tuple2<NullWritable, WARCWritable>> {

    SimpleDateFormat _dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    
    private static String SOFTWARE = "flink-crawler";

    private String _userAgentString;

    private boolean _firstRecord = true;
    
    public CreateWARCWritableFunction(String userAgentString) {
       _userAgentString = userAgentString;
    }
    
    @Override
    public void flatMap(FetchResultUrl fetchResultUrl, Collector<Tuple2<NullWritable, WARCWritable>> collector)
            throws Exception {
        
        if (_firstRecord ) {
            outputWARCInfoRecord(collector);
            _firstRecord = false;
        }
        
        outputWARCResourceRecord(collector, fetchResultUrl);
    }

    private void outputWARCResourceRecord(Collector<Tuple2<NullWritable, WARCWritable>> collector,
            FetchResultUrl fetchResultUrl) throws IOException {
        
        byte[] content = fetchResultUrl.getContent();
        
        StringBuffer buffer = new StringBuffer();
        buffer.append("WARC/1.0\r\n");
        buffer.append("WARC-Type: resource\r\n");
        buffer.append(String.format("WARC-Target-URI: %s\r\n", fetchResultUrl.getUrl())); 
        buffer.append(String.format("WARC-Date: %s\r\n", _dateFormat.format(new Date())));
        buffer.append("WARC-Record-ID: <urn:uuid:d9bbb325-c09f-473c-8600-1c9dbd4ec443>\r\n"); // TODO create uuid ?
        buffer.append(String.format("Content-Type: %s\r\n", fetchResultUrl.getContentType()));
        buffer.append(String.format("Content-Length: %d\r\n", content.length));
        buffer.append("\r\n");
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream( );
        bos.write(buffer.toString().getBytes("UTF-8") );
        bos.write(content);
        bos.write("\r\n\r\n\r\n".getBytes("UTF-8"));
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        bos.close();
        WARCRecord record = new WARCRecord(stream);
        WARCWritable writable = new WARCWritable(record);
        collector.collect(new Tuple2<NullWritable, WARCWritable>(NullWritable.get(), writable));
        
    }
    private void outputWARCInfoRecord(Collector<Tuple2<NullWritable, WARCWritable>> collector) throws IOException {

        StringBuffer content = new StringBuffer();

        content.append(String.format("software: %s\r\n", SOFTWARE)); 
        content.append(String.format("http-header-user-agent': %s\r\n", _userAgentString));
        content.append("format: WARC File Format 1.0\r\n");
        content.append("conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf\r\n");
        
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
