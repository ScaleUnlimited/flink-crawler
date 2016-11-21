package com.scaleunlimited.flinkcrawler.tools;

import static org.junit.Assert.*;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.junit.Test;

import com.scaleunlimited.flinkcrawler.crawldb.SimpleCrawlDB;
import com.scaleunlimited.flinkcrawler.fetcher.MockRobotsFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.SimpleFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.UserAgent;
import com.scaleunlimited.flinkcrawler.fetcher.WebGraphFetcher;
import com.scaleunlimited.flinkcrawler.functions.CheckUrlWithRobotsFunction;
import com.scaleunlimited.flinkcrawler.functions.CrawlDBFunction;
import com.scaleunlimited.flinkcrawler.parser.SimplePageParser;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.robots.SimpleRobotsParser;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.tools.CrawlTopology.CrawlTopologyBuilder;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;
import com.scaleunlimited.flinkcrawler.webgraph.SimpleWebGraph;

public class CrawlTopologyTest {

	@Test
	public void test() throws Exception {
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		SimpleWebGraph graph = new SimpleWebGraph(new SimpleUrlNormalizer())
			.add("domain1.com", "domain1.com/page1", "domain1.com/page2")
			.add("domain1.com/page1")
			.add("domain1.com/page2", "domain2.com", "domain1.com", "domain1.com/page1")
			.add("domain2.com", "domain2.com/page1");

		CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env)
			.setUrlSource(new SeedUrlSource(1.0f, "http://domain1.com"))
			.setUrlLengthener(new SimpleUrlLengthener())
			.setCrawlDB(new SimpleCrawlDB())
			.setRobotsFetcher(new MockRobotsFetcher())
			.setRobotsParser(new SimpleRobotsParser())
			.setPageParser(new SimplePageParser())
			.setContentSink(new DiscardingSink<ParsedUrl>())
			.setUrlNormalizer(new SimpleUrlNormalizer())
			.setUrlFilter(new SimpleUrlValidator())
			.setPageFetcher(new WebGraphFetcher(graph))
			.setRunTime(5000);

		builder.build().execute();
	}
}
