package com.scaleunlimited.flinkcrawler.pojos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import com.scaleunlimited.flinkcrawler.crawldb.IPayload;

import crawlercommons.domains.PaidLevelDomain;

@SuppressWarnings("serial")
public class ValidUrl extends BaseUrl implements IPayload {
	
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
			_port = url.getPort();
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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(_protocol);
		out.writeUTF(_hostname);
		out.writeUTF(_pld);
		out.writeInt(_port);
		out.writeUTF(_path);
		
		if (_query == null) {
			out.writeUTF("");
		} else {
			out.writeUTF(_query);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		_protocol = in.readUTF();
		_hostname = in.readUTF();
		_pld = in.readUTF();
		_port = in.readInt();
		_path = in.readUTF();
		_query = in.readUTF();
		if (_query.isEmpty()) {
			_query = null;
		}
		
		// Now we need to reconstruct the original URL.
		// TODO use our own method to build this if it's http or https, so we
		// don't have to create a URL. Remember to exclude port if it's -1.
		String file = _query == null ? _path : _path + "?" + _query;
		URL url = new URL(_protocol, _hostname, _port, file);
		setUrlAsString(url.toString());
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((_hostname == null) ? 0 : _hostname.hashCode());
		result = prime * result + ((_path == null) ? 0 : _path.hashCode());
		result = prime * result + ((_pld == null) ? 0 : _pld.hashCode());
		result = prime * result + _port;
		result = prime * result
				+ ((_protocol == null) ? 0 : _protocol.hashCode());
		result = prime * result + ((_query == null) ? 0 : _query.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ValidUrl other = (ValidUrl) obj;
		if (_hostname == null) {
			if (other._hostname != null)
				return false;
		} else if (!_hostname.equals(other._hostname))
			return false;
		if (_path == null) {
			if (other._path != null)
				return false;
		} else if (!_path.equals(other._path))
			return false;
		if (_pld == null) {
			if (other._pld != null)
				return false;
		} else if (!_pld.equals(other._pld))
			return false;
		if (_port != other._port)
			return false;
		if (_protocol == null) {
			if (other._protocol != null)
				return false;
		} else if (!_protocol.equals(other._protocol))
			return false;
		if (_query == null) {
			if (other._query != null)
				return false;
		} else if (!_query.equals(other._query))
			return false;
		return true;
	}

	
}
