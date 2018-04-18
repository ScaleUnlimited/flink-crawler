package com.scaleunlimited.flinkcrawler.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.config.CrawlTerminator;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings("serial")
public class NoActivityCrawlTerminator extends CrawlTerminator {
    static final Logger LOGGER = LoggerFactory.getLogger(NoActivityCrawlTerminator.class);

    private long _maxQuietTimeMS;
    
    public NoActivityCrawlTerminator(long maxQuietTimeMS) {
        _maxQuietTimeMS = maxQuietTimeMS;
    }
    
    @Override
    public void open() {
        super.open();
        
        UrlLogger.resetActivityTime();
    }
    
    @Override
    public boolean isTerminated() {
        long lastActivityTime = UrlLogger.getLastActivityTime();
        if (lastActivityTime != UrlLogger.NO_ACTIVITY_TIME) {
            long curTime = System.currentTimeMillis();
            if ((curTime - lastActivityTime) > _maxQuietTimeMS) {
                LOGGER.info("Terminating seed URL source due to lack of activity");
                return true;
            }
        }
        
        return false;
    }
    
}

