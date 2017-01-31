package com.scaleunlimited.flinkcrawler.pojos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class BaseUrl implements Serializable {
	
	private String _url;
	
	public BaseUrl() {
		// Constructor so it's a valid POJO
	}
	
	public BaseUrl(String urlAsString) {
		_url = urlAsString;
	}
	
	public BaseUrl(BaseUrl base) {
		_url = base.getUrl();
	}
	
	public void setUrlAsString(String urlAsString) {
		_url = urlAsString;
	}
	
	public String getUrl() {
		return _url;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(_url);
	}

	public void readFields(DataInput in) throws IOException {
		_url = in.readUTF();
	}
	
	public void clear() {
		_url = null;
	}
	
	@Override
	public String toString() {
		return _url;
	}

}
