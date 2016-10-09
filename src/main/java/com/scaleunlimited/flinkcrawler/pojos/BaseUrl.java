package com.scaleunlimited.flinkcrawler.pojos;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class BaseUrl implements Serializable {

	private boolean _tickle = false;
	
	public BaseUrl() { }

	public BaseUrl(boolean tickle) {
		_tickle = tickle;
	}
	
	public BaseUrl setTickle(boolean tickle) {
		_tickle = tickle;
		return this;
	}
	
	public boolean isTickle() {
		return _tickle;
	}
}
