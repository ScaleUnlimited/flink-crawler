package com.scaleunlimited.flinkcrawler.tools;

import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.junit.Test;

import com.scaleunlimited.flinkcrawler.crawldb.InMemoryCrawlDB;
import com.scaleunlimited.flinkcrawler.fetcher.MockRobotsFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.WebGraphFetcher;
import com.scaleunlimited.flinkcrawler.functions.CheckUrlWithRobotsFunction;
import com.scaleunlimited.flinkcrawler.functions.CrawlDBFunction;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseFunction;
import com.scaleunlimited.flinkcrawler.parser.SimplePageParser;
import com.scaleunlimited.flinkcrawler.pojos.BaseUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.robots.SimpleRobotsParser;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.tools.CrawlTopology.CrawlTopologyBuilder;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;
import com.scaleunlimited.flinkcrawler.webgraph.SimpleWebGraph;

public class CrawlTopologyTest {

	@Test
	public void test() throws Exception {
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
		SimpleWebGraph graph = new SimpleWebGraph(normalizer)
			.add("domain1.com", "domain1.com/page1", "domain1.com/page2", "domain1.com/blocked")
			.add("domain1.com/page1")
			.add("domain1.com/page2", "domain2.com", "domain1.com", "domain1.com/page1")
			.add("domain2.com", "domain2.com/page1");

		CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env)
			.setUrlSource(new SeedUrlSource(1.0f, "http://domain1.com"))
			.setUrlLengthener(new SimpleUrlLengthener())
			.setCrawlDB(new InMemoryCrawlDB())
			.setRobotsFetcher(new MockRobotsFetcher())
			.setRobotsParser(new SimpleRobotsParser())
			.setPageParser(new SimplePageParser())
			.setContentSink(new DiscardingSink<ParsedUrl>())
			.setUrlNormalizer(normalizer)
			.setUrlFilter(new SimpleUrlValidator())
			.setPageFetcher(new WebGraphFetcher(graph));

		builder.build().execute();
		
		// TODO add support for validating calls (e.g.n calls to class x, or class x called with y,z,blah values)
		for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : UrlLogger.getLog()) {
			System.out.format("%s: %s\n", entry.f0, entry.f1);
		}
		
		UrlLogger.getResults()
			.assertUrlLoggedBy(FetchUrlsFunction.class, normalizer.normalize("domain1.com/page1"), 1, FetchStatus.class.getSimpleName(), FetchStatus.FETCHED.toString())
			
			.assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, normalizer.normalize("domain2.com/page1"), 1)
			.assertUrlLoggedBy(FetchUrlsFunction.class, normalizer.normalize("domain2.com/page1"), 1)
			.assertUrlNotLoggedBy(ParseFunction.class, normalizer.normalize("domain2.com/page1"))
			
			.assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, normalizer.normalize("domain1.com/blocked"), 1)
			.assertUrlNotLoggedBy(FetchUrlsFunction.class, normalizer.normalize("domain1.com/blocked"))
			.assertUrlLoggedBy(CrawlDBFunction.class, normalizer.normalize("domain1.com/blocked"), 2);
	}
}
