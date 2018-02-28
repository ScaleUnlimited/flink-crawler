package com.scaleunlimited.flinkcrawler.urls;

import java.io.Serializable;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;

@SuppressWarnings("serial")
public abstract class BaseUrlLengthener implements Serializable {
    
    public abstract void open() throws Exception;

	public abstract RawUrl lengthen(RawUrl url);

	public abstract int getTimeoutInSeconds();
	
}
