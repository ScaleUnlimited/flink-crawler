package com.scaleunlimited.flinkcrawler.tools;

import java.io.IOException;
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

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.NoopHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.SimpleHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.commoncrawl.CommonCrawlFetcherBuilder;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.topology.CrawlTopologyBuilder;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.NoopUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;
import com.scaleunlimited.flinkcrawler.urls.SingleDomainUrlValidator;

import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.sitemaps.SiteMapParser;

public class CrawlTool {

    public static final long DO_NOT_FORCE_CRAWL_DELAY = -1L;

    // As per https://developers.google.com/search/reference/robots_txt
    private static final int MAX_ROBOTS_TXT_SIZE = 500 * 1024;

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

        BaseUrlLengthener urlLengthener = getUrlLengthener(options, userAgent);
        BaseHttpFetcherBuilder siteMapFetcherBuilder = getSitemapFetcherBuilder(options, userAgent);
        BaseHttpFetcherBuilder robotsFetcherBuilder = getRobotsFetcherBuilder(options, userAgent);
        BaseHttpFetcherBuilder pageFetcherBuilder = getPageFetcherBuilder(options, userAgent);

        // See if we need to restrict what mime types we download.
        if (options.isHtmlOnly()) {
            Set<String> validMimeTypes = new HashSet<>();
            for (MediaType mediaType : new HtmlParser().getSupportedTypes(new ParseContext())) {
                validMimeTypes.add(mediaType.toString());
            }

            pageFetcherBuilder.setValidMimeTypes(validMimeTypes);
        }

        CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env).setUserAgent(userAgent)
                .setUrlLengthener(urlLengthener)
                .setUrlSource(new SeedUrlSource(options.getSeedUrlsFilename(), RawUrl.DEFAULT_SCORE))
                .setRobotsFetcherBuilder(robotsFetcherBuilder).setUrlFilter(urlValidator)
                .setSiteMapFetcherBuilder(siteMapFetcherBuilder)
                .setPageFetcherBuilder(pageFetcherBuilder)
                .setForceCrawlDelay(options.getForceCrawlDelay())
                .setDefaultCrawlDelay(options.getDefaultCrawlDelay())
                .setParallelism(options.getParallelism())
                .setMaxOutlinksPerPage(options.getMaxOutlinksPerPage());

        if (options.getOutputFile() != null) {
            builder.setContentTextFile(options.getOutputFile());
        }

        builder.build().execute();
    }

    private static BaseUrlLengthener getUrlLengthener(CrawlToolOptions options, UserAgent userAgent) {
        if (options.isNoLengthen()) {
            return new NoopUrlLengthener();
        }

        int maxConnectionsPerHost = options.getFetchersPerTask();
        return new SimpleUrlLengthener(userAgent, maxConnectionsPerHost);
    }

    private static BaseHttpFetcherBuilder getPageFetcherBuilder(CrawlToolOptions options,
            UserAgent userAgent) throws IOException {
        if (options.isCommonCrawl()) {
            return new CommonCrawlFetcherBuilder(options.getFetchersPerTask(), userAgent,
                    options.getCommonCrawlId(), options.getCommonCrawlCacheDir());
        }

        return new SimpleHttpFetcherBuilder(options.getFetchersPerTask(), userAgent)
                .setDefaultMaxContentSize(options.getMaxContentSize());
    }

    private static BaseHttpFetcherBuilder getSitemapFetcherBuilder(CrawlToolOptions options,
            UserAgent userAgent) throws IOException {
        if (options.isCommonCrawl()) {
            // Common crawl index doesn't have sitemap files.
            return new NoopHttpFetcherBuilder(userAgent);
        }

        // By default, give site map fetcher 20% of #threads page fetcher has
        int maxSimultaneousRequests = Math.max(1, options.getFetchersPerTask() / 5);
        return new SimpleHttpFetcherBuilder(maxSimultaneousRequests, userAgent)
                .setDefaultMaxContentSize(SiteMapParser.MAX_BYTES_ALLOWED);
    }

    private static BaseHttpFetcherBuilder getRobotsFetcherBuilder(CrawlToolOptions options,
            UserAgent userAgent) throws IOException {

        // Although the static Common Crawl data does have robots.txt files
        // (in a separate location), there's no index, so it would be ugly to
        // have to download the whole thing. For now, let's just pretend that
        // nobody has a robots.txt file by using a fetcher that always returns
        // a 404.
        if (options.isCommonCrawl()) {
            return new NoopHttpFetcherBuilder(userAgent);
        }

        // By default, give robots fetcher 20% of #threads page fetcher has
        int maxSimultaneousRequests = Math.max(1, options.getFetchersPerTask() / 5);
        return new SimpleHttpFetcherBuilder(maxSimultaneousRequests, userAgent)
                .setDefaultMaxContentSize(MAX_ROBOTS_TXT_SIZE);
    }

}
