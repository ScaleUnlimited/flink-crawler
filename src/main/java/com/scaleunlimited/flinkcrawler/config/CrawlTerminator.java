package com.scaleunlimited.flinkcrawler.config;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class CrawlTerminator implements Serializable {

    public void open() {
        
    }
    
    public abstract boolean isTerminated();
}
