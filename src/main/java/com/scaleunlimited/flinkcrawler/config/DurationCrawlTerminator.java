package com.scaleunlimited.flinkcrawler.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class DurationCrawlTerminator extends CrawlTerminator {
    private static Logger LOGGER = LoggerFactory.getLogger(DurationCrawlTerminator.class);
    
    private int _maxDurationSec;
    
    private transient long _crawlEndTime;
    
    public DurationCrawlTerminator(int maxDurationSec) {
        _maxDurationSec = maxDurationSec;
    }
    
    @Override
    public void open() {
        super.open();
        
        _crawlEndTime = System.currentTimeMillis() + (_maxDurationSec * 1000L);
    }
    
    @Override
    public boolean isTerminated() {
        long curTime = System.currentTimeMillis();
        boolean terminate = curTime >= _crawlEndTime;
        
        if (terminate) {
            LOGGER.info("Terminating due to current time ({}) later than termination time ({})",
                    curTime, _crawlEndTime);
        }
        
        return terminate;
    }

}
