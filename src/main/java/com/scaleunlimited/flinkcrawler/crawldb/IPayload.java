package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface IPayload {

	public abstract void write(DataOutput out) throws IOException;
	
	public abstract void readFields(DataInput in) throws IOException;
	
	// Clear out the payload
	public abstract void clear();

}
