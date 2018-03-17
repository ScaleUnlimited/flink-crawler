package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

public class SecondaryIndexTest {

    @Test
    public void testSerDeSupport() throws IOException {
        final String filename = "cdx-00000.gz";
        final long segmentOffset = 10;
        final long segmentLength = 100;
        final int segmentId = 5;
        
        SecondaryIndex sd = new SecondaryIndex(filename, segmentOffset, segmentLength, segmentId);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        sd.write(dos);
        dos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        SecondaryIndex sd2 = new SecondaryIndex();
        sd2.read(dis);
        dis.close();
        
        assertEquals(filename, sd2.getIndexFilename());
        assertEquals(segmentOffset, sd2.getSegmentOffset());
        assertEquals(segmentLength, sd2.getSegmentLength());
        assertEquals(segmentId, sd2.getSegmentId());
    }

}
