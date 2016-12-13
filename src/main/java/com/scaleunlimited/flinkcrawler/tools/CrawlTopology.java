package com.scaleunlimited.flinkcrawler.tools;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB;
import com.scaleunlimited.flinkcrawler.fetcher.BaseFetcher;
import com.scaleunlimited.flinkcrawler.functions.CheckUrlWithRobotsFunction;
import com.scaleunlimited.flinkcrawler.functions.CrawlDBFunction;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.LengthenUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.NormalizeUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.OutlinkToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseFunction;
import com.scaleunlimited.flinkcrawler.functions.RawToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.functions.UrlKeySelector;
import com.scaleunlimited.flinkcrawler.functions.ValidUrlsFilter;
import com.scaleunlimited.flinkcrawler.parser.BasePageParser;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.robots.BaseRobotsParser;
import com.scaleunlimited.flinkcrawler.sources.BaseUrlSource;
import com.scaleunlimited.flinkcrawler.sources.TickleSource;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlValidator;

/**
 * A Flink streaming workflow that can be executed.
 * 
 * State Checkpoints in Iterative Jobs
 * 
 * Flink currently only provides processing guarantees for jobs without iterations. Enabling checkpointing on an
 * iterative job causes an exception. In order to force checkpointing on an iterative program the user needs to set a
 * special flag when enabling checkpointing: env.enableCheckpointing(interval, force = true).
 * 
 * Please note that records in flight in the loop edges (and the state changes associated with them) will be lost during
 * failure.
 * 
 */
public class CrawlTopology {

    private StreamExecutionEnvironment _env;
    private String _jobName;

    protected CrawlTopology(StreamExecutionEnvironment env, String jobName) {
        _env = env;
        _jobName = jobName;
    }

    public JobExecutionResult execute() throws Exception {
        return _env.execute(_jobName);
    }

    public static class CrawlTopologyBuilder {

        private static final int DEFAULT_PARALLELISM = -1;

        private StreamExecutionEnvironment _env;
        private String _jobName = "flink-crawler";
        private long _runTime = TickleSource.INFINITE_RUN_TIME;
        private int _parallelism = DEFAULT_PARALLELISM;

        private BaseUrlSource _urlSource;

        private BaseCrawlDB _crawlDB;
        
        private BaseFetcher _robotsFetcher;
        private BaseRobotsParser _robotsParser;

        private BaseUrlLengthener _urlLengthener;
        private SinkFunction<ParsedUrl> _contentSink;
        private BaseUrlNormalizer _urlNormalizer;
        private BaseUrlValidator _urlFilter;
        private BaseFetcher _pageFetcher;
        private BasePageParser _pageParser;

        public CrawlTopologyBuilder(StreamExecutionEnvironment env) {
            _env = env;
        }

        public CrawlTopologyBuilder setJobName(String jobName) {
            _jobName = jobName;
            return this;
        }

        public CrawlTopologyBuilder setUrlSource(BaseUrlSource urlSource) {
            _urlSource = urlSource;
            return this;
        }

        public CrawlTopologyBuilder setUrlLengthener(BaseUrlLengthener lengthener) {
            _urlLengthener = lengthener;
            return this;
        }

        public CrawlTopologyBuilder setCrawlDB(BaseCrawlDB crawlDB) {
            _crawlDB = crawlDB;
            return this;
        }

        public CrawlTopologyBuilder setRobotsFetcher(BaseFetcher robotsFetcher) {
            _robotsFetcher = robotsFetcher;
            return this;
        }

        public CrawlTopologyBuilder setRobotsParser(BaseRobotsParser robotsParser) {
        	_robotsParser = robotsParser;
            return this;
        }

        public CrawlTopologyBuilder setPageFetcher(BaseFetcher pageFetcher) {
            _pageFetcher = pageFetcher;
            return this;
        }

        public CrawlTopologyBuilder setPageParser(BasePageParser pageParser) {
            _pageParser = pageParser;
            return this;
        }

        public CrawlTopologyBuilder setContentSink(SinkFunction<ParsedUrl> contentSink) {
            _contentSink = contentSink;
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

        public CrawlTopologyBuilder setRunTime(long runTime) {
        	// TODO this won't work
            _runTime = runTime;
            return this;
        }

        public CrawlTopologyBuilder setParallelism(int parallelism) {
            _parallelism = parallelism;
            return this;
        }

        @SuppressWarnings("serial")
        public CrawlTopology build() {
            // TODO use single topology parallelism? But likely will want different levels for different parts.
            DataStreamSource<RawUrl> rawUrlsSource = _env.addSource(_urlSource);
            if (_parallelism != DEFAULT_PARALLELISM) {
                rawUrlsSource = rawUrlsSource.setParallelism(_parallelism);
            }
            
            KeyedStream<RawUrl, String> rawUrls = rawUrlsSource
            		.name("Seed urls source")
            		.keyBy(new UrlKeySelector<RawUrl>());
            
            // TODO use something like double the fetch timeout here? or add fetch timeout to parse timeout? Maybe we
            // need to be able to ask each operation how long it might take, and use that. Note that as long as the
            // TickleSource keeps pumping out tickle tuples we won't terminate, so we don't need to worry about
            // something like a full CrawlDB merge causing us to time out. So in that case maybe just use double the
            // tickle interval here? But setting it to 200 when tickle is 100 causes us to not terminate :(
            IterativeStream<RawUrl> iteration = rawUrls.iterate(5000);
            DataStream<CrawlStateUrl> cleanedUrls = iteration
            		.keyBy(new UrlKeySelector<RawUrl>())
                    .process(new LengthenUrlsFunction(_urlLengthener)).name("LengthenUrlsFunction")
                    .flatMap(new NormalizeUrlsFunction(_urlNormalizer)).name("NormalizeUrlsFunction")
                    .filter(new ValidUrlsFilter(_urlFilter)).name("FilterUrlsFunction")
                    .map(new RawToStateUrlFunction());

            DataStream<Tuple2<FetchStatus, FetchUrl>> postRobotsUrls = cleanedUrls
            		.keyBy(new UrlKeySelector<CrawlStateUrl>())
            		.process(new CrawlDBFunction(_crawlDB))
            		.keyBy(new UrlKeySelector<FetchUrl>())
                    .process(new CheckUrlWithRobotsFunction(_robotsFetcher, _robotsParser));
            
            // TODO need to split this stream and send rejected URLs back to crawlDB. Probably need to
            // merge this CrawlStateUrl stream with CrawlStateUrl streams from outlinks and fetch results.
            SplitStream<Tuple2<FetchStatus, FetchUrl>> statusOrUrlsToFetch = postRobotsUrls
            		.split(new OutputSelector<Tuple2<FetchStatus, FetchUrl>>() {
                        
            			private final List<String> STATUS_STREAM = Arrays.asList("status");
                        private final List<String> FETCH_URL_STREAM = Arrays.asList("fetch_url");

                        @Override
                        public Iterable<String> select(Tuple2<FetchStatus, FetchUrl> statusOrUrlsToFetch) {
                            if (statusOrUrlsToFetch.f0 != null) {
                                return STATUS_STREAM;
                            } else if (statusOrUrlsToFetch.f1 != null) {
                                return FETCH_URL_STREAM;
                            } else {
                                throw new RuntimeException("Invalid case of neither status nor fetch_url");
                            }
                        }
            		});
            
            // TODO Need to run statusOrUrlsToFetch.select("status") back into cleanedUrlsIterator
            // along with Vivek's fetch failures.
            
            // TODO need a Tuple3 with a FetchedUrl(?) that has status update, which we can merge back in with
            // rejected robots URLs and outlinks. The fetcher code would want to handle settings no outlink(s) or
            // content and just a FetchedUrl for case of a fetch failure or if the content fetched isn't the
            // type that we want (e.g. image file)
            DataStream<Tuple2<ExtractedUrl, ParsedUrl>> fetchedUrls = statusOrUrlsToFetch.select("fetch_url")
            		.map(new MapFunction<Tuple2<FetchStatus,FetchUrl>, FetchUrl>() {

						@Override
						public FetchUrl map(Tuple2<FetchStatus, FetchUrl> justHasFetchUrl)
								throws Exception {
							return justHasFetchUrl.f1;
						}
					})
            		.keyBy(new UrlKeySelector<FetchUrl>())
                    .process(new FetchUrlsFunction(_pageFetcher))
                    .flatMap(new ParseFunction(_pageParser));

            SplitStream<Tuple2<ExtractedUrl, ParsedUrl>> outlinksOrContent = fetchedUrls
                    .split(new OutputSelector<Tuple2<ExtractedUrl, ParsedUrl>>() {

                        private final List<String> OUTLINK_STREAM = Arrays.asList("outlink");
                        private final List<String> CONTENT_STREAM = Arrays.asList("content");

                        @Override
                        public Iterable<String> select(Tuple2<ExtractedUrl, ParsedUrl> outlinksOrContent) {
                            if (outlinksOrContent.f0 != null) {
                                return OUTLINK_STREAM;
                            } else if (outlinksOrContent.f1 != null) {
                                return CONTENT_STREAM;
                            } else {
                                throw new RuntimeException("Invalid case of neither outlink nor content");
                            }
                        }
                    });

            DataStream<RawUrl> newUrls = outlinksOrContent.select("outlink").map(new OutlinkToStateUrlFunction());

            iteration.closeWith(newUrls);

            // Save off parsed page content. So just extract the parsed content piece of the Tuple2, and
            // then pass it on to the provided content sink function.
            outlinksOrContent.select("content").map(new MapFunction<Tuple2<ExtractedUrl, ParsedUrl>, ParsedUrl>() {

                @Override
                public ParsedUrl map(Tuple2<ExtractedUrl, ParsedUrl> in) throws Exception {
                    return in.f1;
                }
            }).addSink(_contentSink);

            return new CrawlTopology(_env, _jobName);
        }

    }
}
