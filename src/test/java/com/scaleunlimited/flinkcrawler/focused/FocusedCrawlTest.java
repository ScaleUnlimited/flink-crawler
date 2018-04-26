package com.scaleunlimited.flinkcrawler.focused;

import java.io.File;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironmentWithAsyncExecution;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.FileUtils;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.fetcher.MockRobotsFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.MockUrlLengthenerFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.SiteMapGraphFetcher;
import com.scaleunlimited.flinkcrawler.fetcher.WebGraphFetcher;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.io.WARCWritable;
import com.scaleunlimited.flinkcrawler.metrics.CrawlerAccumulator;
import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.parser.SimpleSiteMapParser;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.topology.CrawlTopology;
import com.scaleunlimited.flinkcrawler.topology.CrawlTopologyBuilder;
import com.scaleunlimited.flinkcrawler.topology.CrawlTopologyTest;
import com.scaleunlimited.flinkcrawler.topology.NoActivityCrawlTerminator;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.TestUrlLogger.UrlLoggerResults;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;
import com.scaleunlimited.flinkcrawler.webgraph.BaseWebGraph;
import com.scaleunlimited.flinkcrawler.webgraph.ScoredWebGraph;

import crawlercommons.robots.SimpleRobotRulesParser;

public class FocusedCrawlTest {

    static final Logger LOGGER = LoggerFactory.getLogger(CrawlTopologyTest.class);

    @Test
    public void testFocused() throws Exception {
        UrlLogger.clear();

        LocalStreamEnvironment env = new LocalStreamEnvironmentWithAsyncExecution();

        final float minFetchScore = 0.75f;
        SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
        ScoredWebGraph graph = new ScoredWebGraph(normalizer)
                .add("domain1.com", 2.0f, "domain1.com/page1", "domain1.com/page2")

                // This page will get fetched right away, because the two links from domain1.com have score of 1.0f
                .add("domain1.com/page1", 1.0f, "domain1.com/page3", "domain1.com/page4")

                // This page will get fetched right away, because the two links have score of 1.0f
                .add("domain1.com/page2", 1.0f, "domain1.com/page5")

                // This page will never be fetched.
                .add("domain1.com/page3", 1.0f)

                // This page will eventually be fetched. The first inbound link (from page1) has a score of 0.5,
                // and then the next link from page5 adds 1.0 to push us over the threshhold
                .add("domain1.com/page4", 1.0f)

                // This page will fetched right away, because page2 gives all of its score (1.0) to page5
                .add("domain1.com/page5", 1.0f, "domain1.com/page4");

        File testDir = new File("target/FocusedCrawlTest/");
        testDir.mkdirs();
        File textDir = new File(testDir, "text");
        File contentTextFile = new File(textDir, "content.txt");
        if (contentTextFile.exists()) {
            FileUtils.deleteFileOrDirectory(contentTextFile);
        }

        File warcDir = new File(testDir, "warc");
        File warcFile = new File(warcDir, "content-warc");
        if (warcFile.exists()) {
            FileUtils.deleteFileOrDirectory(warcFile);
        }

        final long maxQuietTime = 2_000L;
        SeedUrlSource seedUrlSource = new SeedUrlSource(1.0f, "http://domain1.com");
        seedUrlSource.setTerminator(new NoActivityCrawlTerminator(maxQuietTime));
        
        CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env)
                // Explicitly set parallelism so that it doesn't vary based on # of cores
                .setParallelism(2)
                
                // Set a timeout that is safe during our test, given max latency with checkpointing
                // during a run.
                .setIterationTimeout(2000L)
                
                .setUrlSource(seedUrlSource)
                .setUrlLengthener(new SimpleUrlLengthener(
                        new MockUrlLengthenerFetcher.MockUrlLengthenerFetcherBuilder(
                                new MockUrlLengthenerFetcher())))
                .setRobotsFetcherBuilder(
                        new MockRobotsFetcher.MockRobotsFetcherBuilder(new MockRobotsFetcher()))
                .setRobotsParser(new SimpleRobotRulesParser())
                .setPageParser(new FocusedPageParser(new PageNumberScorer()))
                .setContentFile(warcFile.getAbsolutePath())
                .setContentTextFile(contentTextFile.getAbsolutePath()).setUrlNormalizer(normalizer)
                .setUrlFilter(new SimpleUrlValidator())
                .setSiteMapFetcherBuilder(new SiteMapGraphFetcher.SiteMapGraphFetcherBuilder(
                        new SiteMapGraphFetcher(BaseWebGraph.EMPTY_GRAPH)))
                .setSiteMapParser(new SimpleSiteMapParser()).setDefaultCrawlDelay(0)
                .setPageFetcherBuilder(
                        new WebGraphFetcher.WebGraphFetcherBuilder(new WebGraphFetcher(graph)))
                .setFetchQueue(new FetchQueue(10_000, minFetchScore));

        CrawlTopology ct = builder.build();

        File dotFile = new File(testDir, "topology.dot");
        ct.printDotFile(dotFile);

        // Execute for a maximum of 20 seconds.
        ct.execute(20_000);
        for (Tuple3<Class<?>, String, Map<String, String>> entry : UrlLogger.getLog()) {
            LOGGER.debug("{}: {}", entry.f0, entry.f1);
        }

        String domain1page1 = normalizer.normalize("domain1.com/page1");
        String domain1page2 = normalizer.normalize("domain1.com/page2");
        String domain1page3 = normalizer.normalize("domain1.com/page3");
        String domain1page4 = normalizer.normalize("domain1.com/page4");
        String domain1page5 = normalizer.normalize("domain1.com/page5");

        UrlLoggerResults results = new UrlLoggerResults(UrlLogger.getLog());

        results.assertUrlLoggedBy(FetchUrlsFunction.class, domain1page1, 1)
                .assertUrlLoggedBy(FetchUrlsFunction.class, domain1page2, 1)
                // This page never got a high enough estimated score.
                .assertUrlLoggedBy(FetchUrlsFunction.class, domain1page3, 0)
                .assertUrlLoggedBy(FetchUrlsFunction.class, domain1page4, 1)
                .assertUrlLoggedBy(FetchUrlsFunction.class, domain1page5, 1);
    }

    @SuppressWarnings("serial")
    private static class PageNumberScorer extends BasePageScorer {

        @Override
        public float score(ParserResult parse) {
            String title = parse.getParsedUrl().getTitle();
            return Float.parseFloat(title.substring("Synthetic page - score = ".length()));
        }
    }

}
