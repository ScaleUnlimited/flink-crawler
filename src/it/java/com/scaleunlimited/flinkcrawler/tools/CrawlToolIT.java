package com.scaleunlimited.flinkcrawler.tools;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironmentWithAsyncExecution;
import org.junit.Test;

public class CrawlToolIT {

    @Test
    public void test() throws Exception {
        CrawlToolOptions options = new CrawlToolOptions();
        options.setSeedUrlsFilename("./src/it/resources/farsi-seeds.txt");
        options.setCommonCrawlId("2017-22");
        options.setCommonCrawlCacheDir("./target/test/CrawlToolTest/cc-cache/");
        options.setForceCrawlDelay(0L);
        options.setMaxContentSize(100000);
        options.setWARCContentPath("./target/test/CrawlToolIT/output/cc-farsi-content.txt");
        options.setMaxCrawlDuration(20);
        options.setTimeout(10);
        
        LocalStreamEnvironment env = new LocalStreamEnvironmentWithAsyncExecution();

        CrawlTool.run(env, options);

        // TODO confirm results
    }

}
