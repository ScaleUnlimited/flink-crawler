package com.scaleunlimited.flinkcrawler.tools;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironmentWithAsyncExecution;
import org.junit.Ignore;
import org.junit.Test;

import com.scaleunlimited.flinkcrawler.tools.CrawlTool.CrawlToolOptions;

public class CrawlToolIT {

	// TODO when the test gets terminated, we can re-enable this.
	@Ignore
	@Test
	public void test() throws Exception {
		CrawlToolOptions options = new CrawlToolOptions();
		options.setSeedUrlsFilename("./src/it/resources/farsi-seeds.txt");
		options.setCommonCrawlId("2017-22");
		options.setCommonCrawlCacheDir("./target/test/CrawlToolTest/cc-cache/");
		options.setForceCrawlDelay(0L);
		options.setMaxContentSize(100000);
		options.setOutputFile("./target/test/CrawlToolTest/output/cc-farsi-content.txt");
		
		LocalStreamEnvironment env = new LocalStreamEnvironmentWithAsyncExecution();

		CrawlTool.run(env, options);
		
		// TODO confirm results
	}

}
