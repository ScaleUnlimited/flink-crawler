package com.scaleunlimited.flinkcrawler.tools;

import java.io.File;
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

		CrawlTopology ct = builder.build();
		
		File testDir = new File("target/CrawlTopologyTest/");
		testDir.mkdirs();
		File dotFile = new File(testDir, "topology.dot");
		ct.printDotFile(dotFile);
		
		ct.execute();
		
		for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : UrlLogger.getLog()) {
			System.out.format("%s: %s\n", entry.f0, entry.f1);
		}
		
		String domain1page1 = normalizer.normalize("domain1.com/page1");
		String domain1page2 = normalizer.normalize("domain1.com/page2");
		String domain2page1 = normalizer.normalize("domain2.com/page1");
		String domain1blockedPage = normalizer.normalize("domain1.com/blocked");
		UrlLogger.getResults()
			.assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, domain1page1, 1)
			.assertUrlLoggedBy(FetchUrlsFunction.class, domain1page1, 1)
			.assertUrlLoggedBy(	CrawlDBFunction.class, domain1page1, 1,
								FetchStatus.class.getSimpleName(), FetchStatus.FETCHED.toString())
			.assertUrlLoggedBy(ParseFunction.class, domain1page1)
			
			.assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, domain1page2, 1)
			.assertUrlLoggedBy(FetchUrlsFunction.class, domain1page2, 1)
			.assertUrlLoggedBy(	CrawlDBFunction.class, domain1page2, 1,
								FetchStatus.class.getSimpleName(), FetchStatus.FETCHED.toString())
			.assertUrlLoggedBy(ParseFunction.class, domain1page2)
			
			.assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, domain2page1, 1)
			.assertUrlLoggedBy(FetchUrlsFunction.class, domain2page1, 1)
			.assertUrlLoggedBy(	CrawlDBFunction.class, domain2page1, 1,
								FetchStatus.class.getSimpleName(), FetchStatus.HTTP_NOT_FOUND.toString())
			.assertUrlNotLoggedBy(ParseFunction.class, domain2page1)
			
			.assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, domain1blockedPage)
			.assertUrlNotLoggedBy(FetchUrlsFunction.class, domain1blockedPage)
			.assertUrlNotLoggedBy(ParseFunction.class, domain1blockedPage)
			.assertUrlLoggedBy(CrawlDBFunction.class, domain1blockedPage, 2);
	}
}
