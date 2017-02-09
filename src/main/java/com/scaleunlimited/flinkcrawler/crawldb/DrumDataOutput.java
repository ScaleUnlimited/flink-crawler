package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.DataOutputStream;
import java.io.OutputStream;

public class DrumDataOutput extends DataOutputStream {

	public DrumDataOutput(OutputStream out) {
		super(out);
	}
	
	// TODO need to track bytes written ourselves, as we want to handle > 2GB
	public long getBytesWritten() {
		return written;
	}

}
