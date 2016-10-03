package com.scaleunlimited.flinkcrawler.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Payload {

	public Payload() {
	}
	
	public abstract void write(DataOutput out) throws IOException;
	
	public abstract void readFields(DataInput in) throws IOException;
	
	// Clear out the payload
	public abstract void clear();

}
