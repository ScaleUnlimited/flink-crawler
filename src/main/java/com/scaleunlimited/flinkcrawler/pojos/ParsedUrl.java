package com.scaleunlimited.flinkcrawler.pojos;

public class ParsedUrl {

	private String _text;
	
	public ParsedUrl() { }

	public ParsedUrl(String text) {
		_text = text;
	}

	public String getText() {
		return _text;
	}

	public void setText(String text) {
		_text = text;
	}

}
