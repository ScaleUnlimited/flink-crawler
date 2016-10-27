package com.scaleunlimited.flinkcrawler.pojos;

import java.util.Map;

public class ParsedUrl {

	private String _url;
	private String _hostAddress;
	private String _parsedText;
	private String _language;
	private String _title;
	private Map<String, String> _parsedMeta;

	// TODO extend this to include the passed in scores and status as well
	public ParsedUrl(String url, String hostAddress, String parsedText,
			String language, String title, Map<String, String> parsedMeta) {

		_url = url;
		_hostAddress = hostAddress;
		_parsedText = parsedText;
		_language = language;
		_title = title;
		_parsedMeta = parsedMeta;
	}

	public String getUrl() {
		return _url;
	}

	public void setUrl(String url) {
		_url = url;
	}

	public String getHostAddress() {
		return _hostAddress;
	}

	public void setHostAddress(String hostAddress) {
		_hostAddress = hostAddress;
	}

	public String getParsedText() {
		return _parsedText;
	}

	public void setParsedText(String parsedText) {
		_parsedText = parsedText;
	}

	public String getLanguage() {
		return _language;
	}

	public void setLanguage(String language) {
		_language = language;
	}

	public String getTitle() {
		return _title;
	}

	public void setTitle(String title) {
		_title = title;
	}

	public Map<String, String> getParsedMeta() {
		return _parsedMeta;
	}

	public void setParsedMeta(Map<String, String> parsedMeta) {
		_parsedMeta = parsedMeta;
	}

}
