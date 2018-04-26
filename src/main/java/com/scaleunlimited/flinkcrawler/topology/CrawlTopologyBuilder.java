package com.scaleunlimited.flinkcrawler.topology;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.SimpleHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.functions.CheckUrlWithRobotsFunction;
import com.scaleunlimited.flinkcrawler.functions.DomainDBFunction;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.LengthenUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.NormalizeUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.OutlinkToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseSiteMapFunction;
import com.scaleunlimited.flinkcrawler.functions.PldKeySelector;
import com.scaleunlimited.flinkcrawler.functions.UrlDBFunction;
import com.scaleunlimited.flinkcrawler.functions.ValidUrlsFilter;
import com.scaleunlimited.flinkcrawler.parser.BasePageParser;
import com.scaleunlimited.flinkcrawler.parser.SimpleLinkExtractor;
import com.scaleunlimited.flinkcrawler.parser.SimplePageParser;
import com.scaleunlimited.flinkcrawler.parser.SimpleSiteMapParser;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.tools.CrawlTool;
import com.scaleunlimited.flinkcrawler.urldb.DefaultUrlStateMerger;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlValidator;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.warc.CreateWARCWritableFunction;
import com.scaleunlimited.flinkcrawler.warc.WARCOutputFormat;
import com.scaleunlimited.flinkcrawler.warc.WARCWritable;

import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.robots.SimpleRobotRulesParser;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;

public class CrawlTopologyBuilder {

    private static final UserAgent INVALID_USER_AGENT = new UserAgent(
            "DO NOT USE THIS USER AGENT (i.e., MAKE YOUR OWN)!", "flink-crawler@scaleunlimited.com",
            "https://github.com/ScaleUnlimited/flink-crawler/wiki/Crawler-Policy");

    private static final String TABS_AND_RETURNS_PATTERN = "[\t\r\n]";

    public static final int DEFAULT_PARALLELISM = -1;
    
    // In production usage, it might take as long as 10 minutes for a checkpoint to
    // complete, so we need to wait at least that long.
    public static final long MAX_ITERATION_TIMEOUT = 10 * 60 * 1000L;
    
    private StreamExecutionEnvironment _env;
    private String _jobName = "flink-crawler";
    private int _parallelism = DEFAULT_PARALLELISM;
    private long _forceCrawlDelay = CrawlTool.DO_NOT_FORCE_CRAWL_DELAY;
    private long _defaultCrawlDelay = 10_000L;
    private long _iterationTimeout = MAX_ITERATION_TIMEOUT;
    
    private SeedUrlSource _urlSource = new SeedUrlSource(makeDefaultSeedUrl());;
    private FetchQueue _fetchQueue = new FetchQueue(10_000);

    private BaseHttpFetcherBuilder _robotsFetcherBuilder = new SimpleHttpFetcherBuilder(1,
            INVALID_USER_AGENT);
    private SimpleRobotRulesParser _robotsParser = new SimpleRobotRulesParser();

    private BaseUrlLengthener _urlLengthener = new SimpleUrlLengthener(INVALID_USER_AGENT, 1);
    private SinkFunction<Tuple2<NullWritable, WARCWritable>> _contentSink;
    private String _contentFilePathString;
    private SinkFunction<String> _contentTextSink;
    private String _contentTextFilePathString;
    private BaseUrlNormalizer _urlNormalizer = new SimpleUrlNormalizer();
    private BaseUrlValidator _urlFilter = new SimpleUrlValidator();
    private BaseHttpFetcherBuilder _pageFetcherBuilder = new SimpleHttpFetcherBuilder(1,
            INVALID_USER_AGENT);
    private BaseHttpFetcherBuilder _siteMapFetcherBuilder = new SimpleHttpFetcherBuilder(1,
            INVALID_USER_AGENT);
    private BasePageParser _pageParser = new SimplePageParser();
    private BasePageParser _siteMapParser = new SimpleSiteMapParser();
    private int _maxOutlinksPerPage = SimpleLinkExtractor.DEFAULT_MAX_EXTRACTED_LINKS_SIZE;

    private String _userAgentString;


    public CrawlTopologyBuilder(StreamExecutionEnvironment env) {
        _env = env;
    }

    public CrawlTopologyBuilder setJobName(String jobName) {
        _jobName = jobName;
        return this;
    }

    public CrawlTopologyBuilder setForceCrawlDelay(long forceCrawlDelay) {
        _forceCrawlDelay = forceCrawlDelay;
        return this;
    }

    public CrawlTopologyBuilder setDefaultCrawlDelay(long defaultCrawlDelay) {
        _defaultCrawlDelay = defaultCrawlDelay;
        return this;
    }

    public CrawlTopologyBuilder setUrlSource(SeedUrlSource urlSource) {
        _urlSource = urlSource;
        return this;
    }

    public CrawlTopologyBuilder setUrlLengthener(BaseUrlLengthener lengthener) {
        _urlLengthener = lengthener;
        return this;
    }

    public CrawlTopologyBuilder setRobotsFetcherBuilder(
            BaseHttpFetcherBuilder robotsFetcherBuilder) {
        _robotsFetcherBuilder = robotsFetcherBuilder;
        return this;
    }

    public CrawlTopologyBuilder setRobotsParser(SimpleRobotRulesParser robotsParser) {
        _robotsParser = robotsParser;
        return this;
    }

    public CrawlTopologyBuilder setUserAgent(UserAgent userAgent) {
        _pageFetcherBuilder.setUserAgent(userAgent);
        _siteMapFetcherBuilder.setUserAgent(userAgent);
        _robotsFetcherBuilder.setUserAgent(userAgent);
        _userAgentString = userAgent.getUserAgentString();
        return this;
    }

    public CrawlTopologyBuilder setPageFetcherBuilder(BaseHttpFetcherBuilder pageFetcherBuilder) {
        _pageFetcherBuilder = pageFetcherBuilder;
        return this;
    }

    public CrawlTopologyBuilder setSiteMapFetcherBuilder(
            BaseHttpFetcherBuilder siteMapFetcherBuilder) {
        _siteMapFetcherBuilder = siteMapFetcherBuilder;
        return this;
    }

    public CrawlTopologyBuilder setPageParser(BasePageParser pageParser) {
        _pageParser = pageParser;
        return this;
    }

    public CrawlTopologyBuilder setMaxOutlinksPerPage(int maxOutlinksPerPage) {
        _maxOutlinksPerPage = maxOutlinksPerPage;
        return this;
    }

    public CrawlTopologyBuilder setIterationTimeout(long iterationTimeout) {
        _iterationTimeout = iterationTimeout;
        return this;
    }
    
    public CrawlTopologyBuilder setFetchQueue(FetchQueue fetchQueue) {
        _fetchQueue = fetchQueue;
        return this;
    }

    public CrawlTopologyBuilder setSiteMapParser(BasePageParser siteMapParser) {
        _siteMapParser = siteMapParser;
        return this;
    }

    public CrawlTopologyBuilder setContentSink(SinkFunction<Tuple2<NullWritable, WARCWritable>> contentSink) {
        _contentSink = contentSink;
        return this;
    }

    public CrawlTopologyBuilder setContentFile(String filePathString) {
        if (_contentSink != null) {
            throw new IllegalArgumentException("already have a content sink");
        }
        _contentFilePathString = filePathString;
        return this;
    }


    public CrawlTopologyBuilder setContentTextSink(SinkFunction<String> contentTextSink) {
        if (_contentTextFilePathString != null) {
            throw new IllegalArgumentException("already have a content text file path");
        }
        _contentTextSink = contentTextSink;
        return this;
    }

    public CrawlTopologyBuilder setContentTextFile(String filePathString) {
        if (_contentTextSink != null) {
            throw new IllegalArgumentException("already have a content text sink");
        }
        _contentTextFilePathString = filePathString;
        return this;
    }

    public CrawlTopologyBuilder setUrlNormalizer(BaseUrlNormalizer urlNormalizer) {
        _urlNormalizer = urlNormalizer;
        return this;
    }

    public CrawlTopologyBuilder setUrlFilter(BaseUrlValidator urlValidator) {
        _urlFilter = urlValidator;
        return this;
    }

    public CrawlTopologyBuilder setParallelism(int parallelism) {
        _parallelism = parallelism;
        return this;
    }
    
    public int getRealParallelism() {
        if (_parallelism == DEFAULT_PARALLELISM) {
            return _env.getParallelism();
        } else {
            return _parallelism;
        }
    }

    /**
     * This is where we do all of the heavy lifting to actually use all of our configuration settings to build a
     * CrawlTopology.
     * 
     * @return CrawlTopology that can be executed.
     * @throws IOException 
     */
    @SuppressWarnings("serial")
    public CrawlTopology build() throws IOException {
        if (_parallelism != DEFAULT_PARALLELISM) {
            _env.setParallelism(_parallelism);
        }

        if ((_robotsFetcherBuilder.getUserAgent() == INVALID_USER_AGENT)
                || (_siteMapFetcherBuilder.getUserAgent() == INVALID_USER_AGENT)
                || (_pageFetcherBuilder.getUserAgent() == INVALID_USER_AGENT)) {
            throw new IllegalArgumentException("You must define your own UserAgent!");
        }

        // The Headers class in http-fetcher uses an unmodifiable list, which Kryo can't handle
        // unless we register a special serializer provided by the "kryo-serializers" project.
        _env.registerTypeWithKryoSerializer(
                Collections.unmodifiableList(new ArrayList<>()).getClass(),
                UnmodifiableCollectionsSerializer.class);

        // Make sure the tickler parallelism is set for the urlSource
        _urlSource.setParallelism(getRealParallelism());
        SplitStream<RawUrl> seedUrls = _env.addSource(_urlSource)
                .name("Seed urls source")
                .split(new OutputSelector<RawUrl>() {

                    private final List<String> REGULAR_URLS = Arrays.asList("regular");
                    private final List<String> SPECIAL_URLS = Arrays.asList("special");

                    @Override
                    public Iterable<String> select(RawUrl url) {
                        return url.isRegular() ? REGULAR_URLS : SPECIAL_URLS;
                    }
                });

        // Clean up the regular URLs, and then re-combine (union) with the special
        // URLs that don't need cleaning.
        DataStream<CrawlStateUrl> cleanedUrls = cleanUrls(seedUrls.select("regular"))
                .union(seedUrls.select("special")
                        .map(new MapFunction<RawUrl, CrawlStateUrl>() {

                    @Override
                    public CrawlStateUrl map(RawUrl url) throws Exception {
                        return new CrawlStateUrl(url);
                    }
                }));

        // Update the URL DB, then run URLs it emits through robots filtering.
        IterativeStream<CrawlStateUrl> urlDbIteration = cleanedUrls
                .iterate(_iterationTimeout);
        SingleOutputStreamOperator<CrawlStateUrl> postDomainDbUrls = urlDbIteration
                .keyBy(new PldKeySelector<CrawlStateUrl>())
                .process(new DomainDBFunction())
                .name("DomainDBFunction");
                
        SingleOutputStreamOperator<FetchUrl> postUrlDbUrls = postDomainDbUrls
                .keyBy(new PldKeySelector<CrawlStateUrl>())
                .process(new UrlDBFunction(new DefaultUrlStateMerger(), _fetchQueue))
                .name("UrlDBFunction");

        DataStream<FetchUrl> preRobotsUrls = postUrlDbUrls
                // We have to re-key since process() destroys the keyed state, and CheckUrlWithRobotsFunction
                // needs to process URLs by PLD.
                .keyBy(new PldKeySelector<FetchUrl>());

        DataStream<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> postRobotsUrls = AsyncDataStream
                .unorderedWait(preRobotsUrls,
                        new CheckUrlWithRobotsFunction(_robotsFetcherBuilder, _robotsParser,
                                _forceCrawlDelay, _defaultCrawlDelay),
                        _robotsFetcherBuilder.getFetchDurationTimeoutInSeconds() * 2,
                        TimeUnit.SECONDS)
                .name("CheckUrlWithRobotsFunction");

        // Split this stream into passed, blocked or sitemap.
        SplitStream<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> blockedOrPassedOrSitemapUrls = postRobotsUrls
                .split(new OutputSelector<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>>() {

                    private final List<String> BLOCKED_STREAM = Arrays.asList("blocked");
                    private final List<String> PASSED_STREAM = Arrays.asList("passed");
                    private final List<String> SITEMAP_STREAM = Arrays.asList("sitemap");

                    @Override
                    public Iterable<String> select(
                            Tuple3<CrawlStateUrl, FetchUrl, FetchUrl> blockedOrPassedOrSitemapUrl) {
                        if (blockedOrPassedOrSitemapUrl.f0 != null) {
                            return BLOCKED_STREAM;
                        } else if (blockedOrPassedOrSitemapUrl.f1 != null) {
                            return PASSED_STREAM;
                        } else if (blockedOrPassedOrSitemapUrl.f2 != null) {
                            return SITEMAP_STREAM;
                        } else {
                            throw new RuntimeException(
                                    "Invalid case of neither blocked nor passed nor sitemap");
                        }
                    }
                });

        // Split off the sitemap urls and fetch and later parse them using the sitemap fetcher
        // and parser to generate outlinks
        DataStream<FetchUrl> sitemapUrlsToFetch = blockedOrPassedOrSitemapUrls
                .select("sitemap")
                .map(new MapFunction<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>, FetchUrl>() {

                    @Override
                    public FetchUrl map(Tuple3<CrawlStateUrl, FetchUrl, FetchUrl> sitemapUrl)
                            throws Exception {
                        return sitemapUrl.f2;
                    }
                })
                .name("Select sitemap URLs")
                // So we don't wind up running the async sitemap fetch as part of the async
                // robots fetch/check task.
                .rebalance();

        DataStream<FetchResultUrl> sitemapUrls =
                // TODO get capacity from fetcher builder.
                AsyncDataStream.unorderedWait(sitemapUrlsToFetch,
                        new FetchUrlsFunction(_siteMapFetcherBuilder),
                        _siteMapFetcherBuilder.getFetchDurationTimeoutInSeconds() * 2,
                        TimeUnit.SECONDS, 10000)
                .name("FetchUrlsFunction for sitemap");

        // Run the failed urls into a custom function to log it and then to a DiscardingSink.
        // FUTURE - flag as sitemap and emit as any other url from the robots code; but this would require us to payload
        // the flag through
        DataStream<RawUrl> newSiteMapExtractedUrls = sitemapUrls
                .flatMap(new ParseSiteMapFunction(_siteMapParser))
                .name("ParseSiteMapFunction")
                .map(new OutlinkToStateUrlFunction())
                .name("OutlinkToStateUrlFunction");

        // Split off rejected URLs. These will get unioned (merged) with the status of URLs that we
        // attempt to fetch, and then fed back into the crawl DB via the inner iteration.
        DataStream<CrawlStateUrl> robotBlockedUrls = blockedOrPassedOrSitemapUrls.select("blocked")
                .map(new MapFunction<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>, CrawlStateUrl>() {

                    @Override
                    public CrawlStateUrl map(Tuple3<CrawlStateUrl, FetchUrl, FetchUrl> blockedUrl)
                            throws Exception {
                        return blockedUrl.f0;
                    }
                }).name("Select blocked URLs");

        // Fetch the URLs that passed our robots filter
        KeyedStream<FetchUrl, String> urlsToFetch = blockedOrPassedOrSitemapUrls.select("passed")
                .map(new MapFunction<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>, FetchUrl>() {

                    @Override
                    public FetchUrl map(Tuple3<CrawlStateUrl, FetchUrl, FetchUrl> justHasFetchUrl)
                            throws Exception {
                        return justHasFetchUrl.f1;
                    }
                })
                .name("Select passed URLs")
                // We need to key by PLD since the FetchUrlsFunction has to enforce politeness
                // on a per-domain basis.
                .keyBy(new PldKeySelector<FetchUrl>());

        DataStream<FetchResultUrl> fetchResultUrls =
                // TODO get capacity from fetcher builder.
                AsyncDataStream
                        .unorderedWait(urlsToFetch, new FetchUrlsFunction(_pageFetcherBuilder),
                                _pageFetcherBuilder.getFetchDurationTimeoutInSeconds() * 2,
                                TimeUnit.SECONDS, 10000)
                        .name("FetchUrlsFunction");

        // Save off the content by converting it to a WARC record and passing it on to the provided
        // content sink function.
        DataStream<FetchResultUrl> fetchResultUrlsToSave = fetchResultUrls;
        SingleOutputStreamOperator<Tuple2<NullWritable, WARCWritable>> warcStream = fetchResultUrlsToSave
                .flatMap(new CreateWARCWritableFunction(_userAgentString))
                .name("Create WARC writable");
        
        DataStreamSink<Tuple2<NullWritable, WARCWritable>> contentSinkUsingOutputFormat = null;
        if (_contentSink != null) {
            contentSinkUsingOutputFormat = warcStream.addSink(_contentSink);
        } else {
            Job job = Job.getInstance();
            WARCOutputFormat.setOutputPath(job, new Path(_contentFilePathString));
            HadoopOutputFormat<NullWritable, WARCWritable> hadoopOutputFormat = new HadoopOutputFormat<NullWritable, WARCWritable>(new WARCOutputFormat(), job);
            contentSinkUsingOutputFormat = warcStream.writeUsingOutputFormat(hadoopOutputFormat);
        } 

        contentSinkUsingOutputFormat
                .name("Content Sink");
        
        final int parseParallelism = getRealParallelism() * 4;
        SingleOutputStreamOperator<ParsedUrl> parsedUrls = fetchResultUrls
                .process(new ParseFunction(_pageParser, _maxOutlinksPerPage))
                .name("ParseFunction")
                // Parsing is CPU intensive, so we want to use more slots for it.
                .setParallelism(parseParallelism);

        DataStream<RawUrl> newRawUrls = parsedUrls.getSideOutput(ParseFunction.OUTLINK_OUTPUT_TAG)
                .map(new OutlinkToStateUrlFunction())
                .name("OutlinkToStateUrlFunction")
                .setParallelism(parseParallelism)
                .union(newSiteMapExtractedUrls);

        DataStream<CrawlStateUrl> newUrls = cleanUrls(newRawUrls);

        // We need to merge robotBlockedUrls with the "queued status" stream from putting URLs onto the
        // fetch queue and the "status" stream from the fetch attempts and all of the new URLs from outlinks and sitemaps.
        DataStream<CrawlStateUrl> domainTicklerUrls = postDomainDbUrls.getSideOutput(DomainDBFunction.DOMAIN_TICKLER_TAG);
        DataStream<CrawlStateUrl> queuedStatusUrls = postUrlDbUrls.getSideOutput(UrlDBFunction.STATUS_OUTPUT_TAG);
        DataStream<CrawlStateUrl> fetchStatusUrls = parsedUrls.getSideOutput(ParseFunction.STATUS_OUTPUT_TAG);
        urlDbIteration.closeWith(robotBlockedUrls.union(domainTicklerUrls, queuedStatusUrls, fetchStatusUrls, newUrls));

        // Save off parsed page content text. But first replace all tabs and returns with a space, since we 
        // are outputting one record per line.
        DataStream<String> contentText = parsedUrls
                .map(new MapFunction<ParsedUrl, String>() {
        
                    @Override
                    public String map(ParsedUrl parsedUrl) throws Exception {
                        String contentField = 
                            parsedUrl.getParsedText().replaceAll(TABS_AND_RETURNS_PATTERN, " ");
                        return parsedUrl.getUrl() + "\t" + contentField;
                    }
                    
                })
                .name("Select fetched content text")
                .setParallelism(parseParallelism);

        DataStreamSink<String> contentTextSink;
        if (_contentTextSink != null) {
            contentTextSink = contentText.addSink(_contentTextSink);
        } else if (_contentTextFilePathString != null) {
            contentTextSink = contentText.writeAsText(_contentTextFilePathString, WriteMode.OVERWRITE);
        } else {
            contentTextSink = contentText.print();
        }
        
        contentTextSink.name("ContentTextSink")
            .setParallelism(parseParallelism);

        return new CrawlTopology(_env, _jobName);
    }

    /**
     * Create a snippet of the topology that takes a RawUrl DataStream and applies an async lengthener, then a
     * normalizer and a validator. What we get out is a CrawlStateUrl DataStream.
     * 
     * @param rawUrls
     * @return
     */
    private DataStream<CrawlStateUrl> cleanUrls(DataStream<RawUrl> rawUrls) {
        return AsyncDataStream
                .unorderedWait(rawUrls, new LengthenUrlsFunction(_urlLengthener),
                        _urlLengthener.getTimeoutInSeconds(), TimeUnit.SECONDS)
                .name("LengthenUrlsFunction")
                .flatMap(new NormalizeUrlsFunction(_urlNormalizer))
                .name("NormalizeUrlsFunction")
                .flatMap(new ValidUrlsFilter(_urlFilter))
                .name("ValidUrlsFilter");
    }

    private static RawUrl makeDefaultSeedUrl() {
        try {
            return new RawUrl("http://www.scaleunlimited.com/");
        } catch (MalformedURLException e) {
            throw new RuntimeException("URL should parse just fine?");
        }
    }

}