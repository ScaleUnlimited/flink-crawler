package com.scaleunlimited.flinkcrawler.utils;

import java.io.DataOutputStream;
import java.io.OutputStream;

public class DrumDataOutput extends DataOutputStream {

	public DrumDataOutput(OutputStream out) {
		super(out);
	}
	
	public int getBytesWritten() {
		return written;
	}

}
