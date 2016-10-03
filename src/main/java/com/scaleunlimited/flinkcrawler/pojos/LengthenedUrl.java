package com.scaleunlimited.flinkcrawler.pojos;

public class LengthenedUrl extends RawUrl {

	private String _pld;
	
	public LengthenedUrl(String url, float estimatedScore, String pld) {
		super(url, estimatedScore);
		_pld = pld;
	}

	public String getPLD() {
		return _pld;
	}

	public void setPLD(String pld) {
		_pld = pld;
	}
}
