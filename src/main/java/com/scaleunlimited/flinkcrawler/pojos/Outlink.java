package com.scaleunlimited.flinkcrawler.pojos;

public class Outlink {

	private String _outlink;
	private String _anchor;
	
	public Outlink() { }

	public Outlink(String outlink, String anchor) {
		_outlink = outlink;
		_anchor = anchor;
	}

	public String getOutlink() {
		return _outlink;
	}

	public void setOutlink(String outlink) {
		_outlink = outlink;
	}

	public String getAnchor() {
		return _anchor;
	}

	public void setAnchor(String anchor) {
		_anchor = anchor;
	}


}
