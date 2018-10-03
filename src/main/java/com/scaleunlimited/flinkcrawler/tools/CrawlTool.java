package com.scaleunlimited.flinkcrawler.tools;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.scaleunlimited.flinkcrawler.config.DurationCrawlTerminator;
import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.topology.CrawlTopologyBuilder;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;
import com.scaleunlimited.flinkcrawler.urls.SingleDomainUrlValidator;
import com.scaleunlimited.flinkcrawler.utils.CrawlToolUtils;

import crawlercommons.fetcher.http.UserAgent;

public class CrawlTool {

    public static final long DO_NOT_FORCE_CRAWL_DELAY = -1L;

    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    public static void main(String[] args) {

        // Dump the classpath to stdout to debug artifact version conflicts
        // System.out.println( "Java classpath: "
        // + System.getProperty("java.class.path", "."));

        CrawlToolOptions options = new CrawlToolOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        // Generate topology, run it

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // TODO decide if this is a default option, or a configuration setting from the cmd line
            env.getConfig().enableObjectReuse();

            if (options.getCheckpointDir() != null) {
                // Enable checkpointing every 100 seconds.
                env.enableCheckpointing(100_000L, CheckpointingMode.AT_LEAST_ONCE, true);
                env.setStateBackend(new FsStateBackend(options.getCheckpointDir()));
            }

            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

            run(env, options);
        } catch (Throwable t) {
            System.err.println("Error running CrawlTool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }

    public static void run(StreamExecutionEnvironment env, CrawlToolOptions options)
            throws Exception {

        // TODO Complain if -cachedir is specified when not running locally?

        SimpleUrlValidator urlValidator = (options.isSingleDomain()
                ? new SingleDomainUrlValidator(options.getSingleDomain())
                : new SimpleUrlValidator());

        UserAgent userAgent = (options.isCommonCrawl()
                ? new UserAgent("unused-common-crawl-user-agent", "", "") : options.getUserAgent());

        BaseUrlLengthener urlLengthener = CrawlToolUtils.getUrlLengthener(options, userAgent);
        BaseHttpFetcherBuilder siteMapFetcherBuilder = CrawlToolUtils.getSitemapFetcherBuilder(options, userAgent);
        BaseHttpFetcherBuilder robotsFetcherBuilder = CrawlToolUtils.getRobotsFetcherBuilder(options, userAgent);
        BaseHttpFetcherBuilder pageFetcherBuilder = CrawlToolUtils.getPageFetcherBuilder(options, userAgent);

        // See if we need to restrict what mime types we download.
        if (options.isHtmlOnly()) {
            Set<String> validMimeTypes = new HashSet<>();
            for (MediaType mediaType : new HtmlParser().getSupportedTypes(new ParseContext())) {
                validMimeTypes.add(mediaType.toString());
            }

            pageFetcherBuilder.setValidMimeTypes(validMimeTypes);
        }

        CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env)
                .setUserAgent(userAgent)
                .setUrlLengthener(urlLengthener)
                .setUrlSource(new SeedUrlSource(options.getSeedUrlsFilename(), RawUrl.DEFAULT_SCORE))
                .setCrawlTerminator(new DurationCrawlTerminator(options.getMaxCrawlDurationSec()))
                .setRobotsFetcherBuilder(robotsFetcherBuilder).setUrlFilter(urlValidator)
                .setSiteMapFetcherBuilder(siteMapFetcherBuilder)
                .setPageFetcherBuilder(pageFetcherBuilder)
                .setForceCrawlDelay(options.getForceCrawlDelay())
                .setDefaultCrawlDelay(options.getDefaultCrawlDelay())
                .setParallelism(options.getParallelism())
                .setIterationTimeout(options.getIterationTimeoutSec() * 1000L)
                .setMaxOutlinksPerPage(options.getMaxOutlinksPerPage());

        if (options.getTextContentPathString() != null) {
            builder.setTextContentPath(options.getTextContentPathString());
        }

        builder.build().execute();
    }
}
