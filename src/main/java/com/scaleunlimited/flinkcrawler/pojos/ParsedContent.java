package com.scaleunlimited.flinkcrawler.pojos;

public class ParsedContent {

	private String _text;
	
	public ParsedContent() { }

	public ParsedContent(String text) {
		_text = text;
	}

	public String getText() {
		return _text;
	}

	public void setText(String text) {
		_text = text;
	}

	
}
