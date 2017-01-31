package com.scaleunlimited.flinkcrawler.pojos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import crawlercommons.url.PaidLevelDomain;

@SuppressWarnings("serial")
public class ValidUrl extends BaseUrl {
	
	private String _protocol;
	private String _hostname;
	private String _pld;
	private int _port;
	private String _path;
	private String _query;
	
	public ValidUrl() {
		// Constructor so it's a valid POJO
	}
	
	public ValidUrl(String urlAsString) throws MalformedURLException {
		setUrl(urlAsString);
	}
	
	public ValidUrl(ValidUrl base) {
		super(base);
		
		_protocol = base.getProtocol();
		_hostname = base.getHostname();
		_pld = base.getPld();
		_port = base.getPort();
		_path = base.getPath();
		_query = base.getQuery();
	}
	
	public void setUrl(String urlAsString) throws MalformedURLException {
		super.setUrlAsString(urlAsString);

		if (urlAsString == null) {
			_protocol = null;
			_hostname = null;
			_port = -1;
			_path = null;
			_query = null;
			_pld = null;
		} else {
			URL url = new URL(urlAsString);

			_protocol = url.getProtocol();
			_hostname = url.getHost();
			_port = url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
			_path = url.getPath();
			_query = url.getQuery();

			_pld = PaidLevelDomain.getPLD(_hostname);
		}
	}
	
	public String getProtocol() {
		return _protocol;
	}

	public String getHostname() {
		return _hostname;
	}

	public String getPld() {
		return _pld;
	}

	public int getPort() {
		return _port;
	}

	public String getPath() {
		return _path;
	}

	public String getQuery() {
		return _query;
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		
		out.writeUTF(_protocol);
		out.writeUTF(_hostname);
		out.writeUTF(_pld);
		out.writeInt(_port);
		out.writeUTF(_path);
		out.writeUTF(_query);
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		
		_protocol = in.readUTF();
		_hostname = in.readUTF();
		_pld = in.readUTF();
		_port = in.readInt();
		_path = in.readUTF();
		_query = in.readUTF();
	}
	
	public void clear() {
		try {
			setUrl(null);
		} catch (MalformedURLException e) {
			throw new RuntimeException("Impossible exception!", e);
		}
	}
	
	@Override
	public String toString() {
		return super.toString();
	}

}
