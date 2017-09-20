package com.scaleunlimited.flinkcrawler.pojos;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.junit.Test;

public class CrawlStateUrlTest {

	@Test
	public void testWriteThenRead() throws Exception {
		ValidUrl url = new ValidUrl("http://domain.com?q=s");
		CrawlStateUrl csu = new CrawlStateUrl(url, FetchStatus.FETCHED, 100, 1.0f, 1000);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		csu.write(dos);
		dos.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		DataInputStream dis = new DataInputStream(bais);
		CrawlStateUrl csu2 = new CrawlStateUrl();
		csu2.readFields(dis);
		dis.close();
		
		assertEquals(csu.getUrl(), csu2.getUrl());
	}

}
