package com.scaleunlimited.flinkcrawler.topology;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironmentWithAsyncExecution;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.fetcher.MockRobotsFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.MockUrlLengthenerFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.SiteMapGraphFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.WebGraphFetcher;
import com.scaleunlimited.flinkcrawler.functions.CheckUrlWithRobotsFunction;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseSiteMapFunction;
import com.scaleunlimited.flinkcrawler.functions.UrlDBFunction;
import com.scaleunlimited.flinkcrawler.parser.SimplePageParser;
import com.scaleunlimited.flinkcrawler.parser.SimpleSiteMapParser;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.TestUrlLogger.UrlLoggerResults;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;
import com.scaleunlimited.flinkcrawler.webgraph.SimpleWebGraph;

import crawlercommons.robots.SimpleRobotRulesParser;

public class CrawlTopologyTest {
    static final Logger LOGGER = LoggerFactory.getLogger(CrawlTopologyTest.class);

    private static final String CRLF = "\r\n";

    @SuppressWarnings("deprecation")
    @Test
    public void testAsync() throws Exception {
        UrlLogger.clear();

        LocalStreamEnvironmentWithAsyncExecution env = new LocalStreamEnvironmentWithAsyncExecution();

        // Set up for checkpointing with in-memory state. Our test runs for several seconds, so using
        // a 1 second interval for checkpointing is sufficient, and using a lower value (like 100ms)
        // sometimes causes the test to fail, because checkpointing happens before all operators have
        // finished deploying, and that in turn causes iterations to not start running properly (or
        // so the logs seem to indicate).
        env.setStateBackend(new MemoryStateBackend());
        env.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE, true);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(20_000L);

        SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
        SimpleWebGraph graph = new SimpleWebGraph(normalizer)
                .add("domain1.com", "domain1.com/page1", "domain1.com/page2", "domain1.com/blocked")
                .add("domain1.com/page1")
                .add("domain1.com/page2", "domain2.com", "domain1.com", "domain1.com/page1")
                .add("domain2.com", "domain2.com/page1", "domain3.com")
                .add("domain3.com", "domain3.com/page1", "bit.ly/domain4.com").add("domain4.com");

        SimpleWebGraph sitemapGraph = new SimpleWebGraph(normalizer).add("domain4.com/sitemap.txt",
                "domain4.com/page1", "domain4.com/page2");

        Map<String, String> robotPages = new HashMap<String, String>();
        // Block one page, and set no crawl delay.
        robotPages.put(normalizer.normalize("http://domain1.com/robots.txt"),
                "User-agent: *" + CRLF + "Disallow: /blocked" + CRLF + "Crawl-delay: 0" + CRLF);

        // Set a long crawl delay.
        robotPages.put("http://domain3.com:80/robots.txt",
                "User-agent: *" + CRLF + "Crawl-delay: 30" + CRLF);
        robotPages.put(normalizer.normalize("http://domain3.com/robots.txt"),
                "User-agent: *" + CRLF + "Crawl-delay: 30" + CRLF);

        // And one with a sitemap
        robotPages.put(normalizer.normalize("http://domain4.com:80/robots.txt"),
                "User-agent: *" + CRLF + "sitemap : http://domain4.com/sitemap.txt");

        // We've got a single URL that needs to get lengthened by our mock lengthener
        Map<String, String> redirections = new HashMap<String, String>();
        redirections.put(normalizer.normalize("bit.ly/domain4.com"),
                normalizer.normalize("domain4.com"));

        File testDir = new File("target/CrawlTopologyTest/");
        testDir.mkdirs();
        File contentTextFile = new File(testDir, "content.txt");
        if (contentTextFile.exists()) {
            FileUtils.deleteFileOrDirectory(contentTextFile);
        }

        final long maxQuietTime = 4_000L;
        final long iterationTimeout = 4_000L;
        SeedUrlSource seedUrlSource = new SeedUrlSource(1.0f, "http://domain1.com");
        seedUrlSource.setTerminator(new NoActivityCrawlTerminator(maxQuietTime));
        
        CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env)
                // Explicitly set parallelism so that it doesn't vary based on # of cores
                .setParallelism(3)
                
                // Set a timeout that is safe during our test, given max latency with checkpointing
                // during a run.
                .setIterationTimeout(iterationTimeout)
                
                .setUrlSource(seedUrlSource)
                .setUrlLengthener(new SimpleUrlLengthener(
                        new MockUrlLengthenerFetcher.MockUrlLengthenerFetcherBuilder(
                                new MockUrlLengthenerFetcher(redirections))))
                .setFetchQueue(new FetchQueue(1_000))
                .setRobotsFetcherBuilder(new MockRobotsFetcher.MockRobotsFetcherBuilder(
                        new MockRobotsFetcher(robotPages)))
                .setRobotsParser(new SimpleRobotRulesParser()).setPageParser(new SimplePageParser())
                .setContentSink(new DiscardingSink<ParsedUrl>())
                .setContentTextFile(contentTextFile.getAbsolutePath()).setUrlNormalizer(normalizer)
                .setUrlFilter(new SimpleUrlValidator())
                
                // Create MockSitemapFetcher - that will return a valid sitemap
                .setSiteMapFetcherBuilder(new SiteMapGraphFetcher.SiteMapGraphFetcherBuilder(
                        new SiteMapGraphFetcher(sitemapGraph)))
                .setSiteMapParser(new SimpleSiteMapParser()).setDefaultCrawlDelay(0)
                .setPageFetcherBuilder(
                        new WebGraphFetcher.WebGraphFetcherBuilder(new WebGraphFetcher(graph)));

        CrawlTopology ct = builder.build();

        File dotFile = new File(testDir, "topology.dot");
        ct.printDotFile(dotFile);

        // Execute for a maximum of 20 seconds.
        ct.execute(20_000L);
        // ct.execute(200_000);

        for (Tuple3<Class<?>, String, Map<String, String>> entry : UrlLogger.getLog()) {
            LOGGER.debug("{}: {}", entry.f0, entry.f1);
        }

        String domain1page1 = normalizer.normalize("domain1.com/page1");
        String domain1page2 = normalizer.normalize("domain1.com/page2");
        String domain2page1 = normalizer.normalize("domain2.com/page1");
        String domain1blockedPage = normalizer.normalize("domain1.com/blocked");
        String domain3deferredPage = normalizer.normalize("domain3.com/page1");
        String domain4SiteMap = normalizer.normalize("domain4.com/sitemap.txt");
        String domain4page1 = normalizer.normalize("domain4.com/page1");
        String domain4page2 = normalizer.normalize("domain4.com/page2");

        UrlLoggerResults results = new UrlLoggerResults(UrlLogger.getLog());

        results.assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, domain1page1, 1)
                .assertUrlLoggedBy(FetchUrlsFunction.class, domain1page1, 1)
                .assertUrlLoggedBy(UrlDBFunction.class, domain1page1, 1,
                        FetchStatus.class.getSimpleName(), FetchStatus.FETCHING.toString())
                .assertUrlLoggedBy(UrlDBFunction.class, domain1page1, 1,
                        FetchStatus.class.getSimpleName(), FetchStatus.FETCHED.toString())
                .assertUrlLoggedBy(ParseFunction.class, domain1page1)

                .assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, domain1page2, 1)
                .assertUrlLoggedBy(FetchUrlsFunction.class, domain1page2, 1)
                .assertUrlLoggedBy(UrlDBFunction.class, domain1page2, 1,
                        FetchStatus.class.getSimpleName(), FetchStatus.FETCHING.toString())
                .assertUrlLoggedBy(UrlDBFunction.class, domain1page2, 1,
                        FetchStatus.class.getSimpleName(), FetchStatus.FETCHED.toString())
                .assertUrlLoggedBy(ParseFunction.class, domain1page2)

                .assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, domain2page1, 1)
                .assertUrlLoggedBy(FetchUrlsFunction.class, domain2page1, 1)
                .assertUrlLoggedBy(UrlDBFunction.class, domain2page1, 1,
                        FetchStatus.class.getSimpleName(), FetchStatus.FETCHING.toString())
                .assertUrlLoggedBy(UrlDBFunction.class, domain2page1, 1,
                        FetchStatus.class.getSimpleName(), FetchStatus.HTTP_NOT_FOUND.toString())
                .assertUrlNotLoggedBy(ParseFunction.class, domain2page1)

                // domain3.com/page1 should be skipped due to crawl-delay.
                .assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, domain3deferredPage, 1)
                .assertUrlLoggedBy(FetchUrlsFunction.class, domain3deferredPage, 1)
                .assertUrlLoggedBy(UrlDBFunction.class, domain3deferredPage, 1,
                        FetchStatus.class.getSimpleName(),
                        FetchStatus.SKIPPED_CRAWLDELAY.toString())
                .assertUrlNotLoggedBy(ParseFunction.class, domain3deferredPage)

                .assertUrlLoggedBy(CheckUrlWithRobotsFunction.class, domain1blockedPage)
                .assertUrlNotLoggedBy(FetchUrlsFunction.class, domain1blockedPage)
                .assertUrlNotLoggedBy(ParseFunction.class, domain1blockedPage)
                .assertUrlLoggedBy(UrlDBFunction.class, domain1blockedPage, 1,
                        FetchStatus.class.getSimpleName(), FetchStatus.FETCHING.toString())
                .assertUrlLoggedBy(UrlDBFunction.class, domain1blockedPage, 1,
                        FetchStatus.class.getSimpleName(), FetchStatus.SKIPPED_BLOCKED.toString())

                .assertUrlLoggedBy(ParseSiteMapFunction.class, domain4SiteMap)
                .assertUrlLoggedBy(UrlDBFunction.class, domain4page1, 1,
                        FetchStatus.class.getSimpleName(), FetchStatus.HTTP_NOT_FOUND.toString())
                .assertUrlLoggedBy(UrlDBFunction.class, domain4page2, 1,
                        FetchStatus.class.getSimpleName(), FetchStatus.HTTP_NOT_FOUND.toString())

        ;
    }
    
}
