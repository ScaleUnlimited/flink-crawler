package com.scaleunlimited.flinkcrawler.tools;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
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

import com.scaleunlimited.flinkcrawler.config.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB;
import com.scaleunlimited.flinkcrawler.functions.CheckUrlWithRobotsFunction;
import com.scaleunlimited.flinkcrawler.functions.CrawlDBFunction;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.LengthenUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.NormalizeUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.OutlinkToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseFunction;
import com.scaleunlimited.flinkcrawler.functions.PldKeySelector;
import com.scaleunlimited.flinkcrawler.functions.UrlKeySelector;
import com.scaleunlimited.flinkcrawler.functions.ValidUrlsFilter;
import com.scaleunlimited.flinkcrawler.parser.BasePageParser;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.BaseUrlSource;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlValidator;
import com.scaleunlimited.flinkcrawler.utils.FlinkUtils;

import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.robots.SimpleRobotRulesParser;

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

    public void printDotFile(File outputFile) throws IOException {
    	String dotAsString = FlinkUtils.planToDot(_env.getExecutionPlan());
    	FileUtils.write(outputFile, dotAsString, "UTF-8");
    }
    
    public JobExecutionResult execute() throws Exception {
        return _env.execute(_jobName);
    }

    public static class CrawlTopologyBuilder {

        private static final int DEFAULT_PARALLELISM = -1;

        private StreamExecutionEnvironment _env;
        private String _jobName = "flink-crawler";
        private int _parallelism = DEFAULT_PARALLELISM;
        private long _maxWaitTime = 5000;
        private long _defaultCrawlDelay = 10 * 1000L;
        
        private BaseUrlSource _urlSource;

        private BaseCrawlDB _crawlDB;
        
        private BaseHttpFetcherBuilder _robotsFetcherBuilder;
        private SimpleRobotRulesParser _robotsParser;

        private BaseUrlLengthener _urlLengthener;
        private SinkFunction<ParsedUrl> _contentSink;
        private BaseUrlNormalizer _urlNormalizer;
        private BaseUrlValidator _urlFilter;
        private BaseHttpFetcherBuilder _pageFetcherBuilder;
        private BasePageParser _pageParser;

        public CrawlTopologyBuilder(StreamExecutionEnvironment env) {
            _env = env;
        }

        public CrawlTopologyBuilder setJobName(String jobName) {
            _jobName = jobName;
            return this;
        }

        public CrawlTopologyBuilder setMaxWaitTime(long maxWaitTime) {
        	_maxWaitTime = maxWaitTime;
            return this;
        }

        public CrawlTopologyBuilder setDefaultCrawlDelay(long defaultCrawlDelay) {
        	_defaultCrawlDelay = defaultCrawlDelay;
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

        public CrawlTopologyBuilder setRobotsFetcherBuilder(BaseHttpFetcherBuilder robotsFetcherBuilder) {
            _robotsFetcherBuilder = robotsFetcherBuilder;
            return this;
        }

        public CrawlTopologyBuilder setRobotsParser(SimpleRobotRulesParser robotsParser) {
        	_robotsParser = robotsParser;
            return this;
        }

        public CrawlTopologyBuilder setPageFetcherBuilder(BaseHttpFetcherBuilder pageFetcherBuilder) {
            _pageFetcherBuilder = pageFetcherBuilder;
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

        public CrawlTopologyBuilder setParallelism(int parallelism) {
            _parallelism = parallelism;
            return this;
        }

        @SuppressWarnings("serial")
        public CrawlTopology build() {
            // FUTURE use single topology parallelism? But likely will want different levels for different parts.
            DataStreamSource<RawUrl> seedUrlsSource = _env.addSource(_urlSource);
            
            if (_parallelism != DEFAULT_PARALLELISM) {
                seedUrlsSource = seedUrlsSource.setParallelism(_parallelism);
            }
            
            // Key is the full URL, as (a) we don't know that it's a valid URL yet, and (b) after lengthening the
            // domain might change, and (c) we don't have to enforce any per-domain constraints here.
            KeyedStream<RawUrl, String> seedUrls = seedUrlsSource
            		.name("Seed urls source")
            		.keyBy(new UrlKeySelector<RawUrl>());
            
            // FUTURE use something like double the fetch timeout here? or add fetch timeout to parse timeout? Maybe we
            // need to be able to ask each operation how long it might take, and use that. Note that we'd also need to
            // worry about a CrawlDB full merge causing us to time out, unless that's run as a background thread.
            // Easiest might be for now to let the caller set this, so for normal testing this is something very short,
            // but we crank it up under production.
            IterativeStream<RawUrl> newUrlsIteration = seedUrls.iterate(_maxWaitTime);
            DataStream<CrawlStateUrl> cleanedUrls = newUrlsIteration
            		.keyBy(new UrlKeySelector<RawUrl>())
            		// TODO LengthenUrlsFunction needs to just pass along invalid URLs
                    .process(new LengthenUrlsFunction(_urlLengthener))
                    .name("LengthenUrlsFunction")
                    .flatMap(new NormalizeUrlsFunction(_urlNormalizer))
                    .name("NormalizeUrlsFunction")
                    .flatMap(new ValidUrlsFilter(_urlFilter))
                    .name("ValidUrlsFilter")
                    .name("RawToStateUrlFunction");
            
            // Update the Crawl DB, then run URLs it emits through robots filtering.
            IterativeStream<CrawlStateUrl> crawlDbIteration = cleanedUrls.iterate(_maxWaitTime);
            DataStream<Tuple2<CrawlStateUrl, FetchUrl>> postRobotsUrls = crawlDbIteration
            		.keyBy(new PldKeySelector<CrawlStateUrl>())
            		.process(new CrawlDBFunction(_crawlDB))
            		.name("CrawlDBFunction")
            		.keyBy(new PldKeySelector<FetchUrl>())
                    .process(new CheckUrlWithRobotsFunction(_robotsFetcherBuilder, _robotsParser, _defaultCrawlDelay))
                    .name("CheckUrlWithRobotsFunction");
            
            // Split this stream into passed and blocked.
            SplitStream<Tuple2<CrawlStateUrl, FetchUrl>> blockedOrPassedUrls = postRobotsUrls
            		.split(new OutputSelector<Tuple2<CrawlStateUrl, FetchUrl>>() {
                        
            			private final List<String> BLOCKED_STREAM = Arrays.asList("blocked");
                        private final List<String> PASSED_STREAM = Arrays.asList("passed");

                        @Override
                        public Iterable<String> select(Tuple2<CrawlStateUrl, FetchUrl> blockedOrPassedUrl) {
                            if (blockedOrPassedUrl.f0 != null) {
                                return BLOCKED_STREAM;
                            } else if (blockedOrPassedUrl.f1 != null) {
                                return PASSED_STREAM;
                            } else {
                                throw new RuntimeException("Invalid case of neither blocked nor passed");
                            }
                        }
            		});
            
            // Split off rejected URLs. These will get unioned (merged) with the status of URLs that we
            // attempt to fetch, and then fed back into the crawl DB via the inner iteration.
            DataStream<CrawlStateUrl> robotBlockedUrls = blockedOrPassedUrls.select("blocked")
            		.map(new MapFunction<Tuple2<CrawlStateUrl,FetchUrl>, CrawlStateUrl>() {

						@Override
						public CrawlStateUrl map(Tuple2<CrawlStateUrl, FetchUrl> blockedUrl)
								throws Exception {
							return blockedUrl.f0;
						}
					})
					.name("Select blocked URLs");
            
            // Fetch the URLs that passed our robots filter
            DataStream<Tuple2<CrawlStateUrl, FetchedUrl>> fetchedUrls = blockedOrPassedUrls.select("passed")
            		.map(new MapFunction<Tuple2<CrawlStateUrl,FetchUrl>, FetchUrl>() {

						@Override
						public FetchUrl map(Tuple2<CrawlStateUrl, FetchUrl> justHasFetchUrl)
								throws Exception {
							return justHasFetchUrl.f1;
						}
					})
					.name("Select passed URLs")
            		.keyBy(new PldKeySelector<FetchUrl>())
                    .process(new FetchUrlsFunction(_pageFetcherBuilder))
                    .name("FetchUrlsFunction");
            
            // Split the fetchedUrls so that we can parse the ones we have actually fetched versus
            // the ones that have failed.
            SplitStream<Tuple2<CrawlStateUrl, FetchedUrl>> fetchAttemptedUrls = fetchedUrls.split(new OutputSelector<Tuple2<CrawlStateUrl, FetchedUrl>>() {
    			private final List<String> FETCH_STATUS_STREAM = Arrays.asList("fetch_status");
                private final List<String> FETCHED_URL_STREAMS = Arrays.asList("fetch_status", "fetched_url");

				@Override
				public Iterable<String> select(Tuple2<CrawlStateUrl, FetchedUrl> url) {
                    if (url.f1 != null) {
                        return FETCHED_URL_STREAMS;
                    } else {
                    	return FETCH_STATUS_STREAM;
                    }
                }
            });
            
            // Get the status of all URLs we've attempted to fetch, union them with URLs blocked by robots, and iterate those back into the crawl DB.
            DataStream<CrawlStateUrl> fetchStatusUrls = fetchAttemptedUrls.select("fetch_status")
            		.map(new MapFunction<Tuple2<CrawlStateUrl, FetchedUrl>, CrawlStateUrl>() {

						@Override
						public CrawlStateUrl map(Tuple2<CrawlStateUrl, FetchedUrl> url) throws Exception {
							return url.f0;
						}
					})
					.name("Select fetch status");

            // We need to merge robotBlockedUrls with the "status" stream from fetchAttemptedUrls
            crawlDbIteration.closeWith(robotBlockedUrls.union(fetchStatusUrls));
            
            DataStream<Tuple2<ExtractedUrl, ParsedUrl>> parsedUrls = fetchAttemptedUrls.select("fetched_url")
            		.map(new MapFunction<Tuple2<CrawlStateUrl, FetchedUrl>, FetchedUrl>() {

						@Override
						public FetchedUrl map( Tuple2<CrawlStateUrl, FetchedUrl> hasFetchedUrl)
								throws Exception {
							return hasFetchedUrl.f1;
						}
            			
            		})
            		.name("Select fetched URLs")
            		.flatMap(new ParseFunction(_pageParser))
            		.name("ParseFunction");
            

            SplitStream<Tuple2<ExtractedUrl, ParsedUrl>> outlinksOrContent = parsedUrls
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

            DataStream<RawUrl> newUrls = outlinksOrContent.select("outlink")
            		.map(new OutlinkToStateUrlFunction())
            		.name("OutlinkToStateUrlFunction");

            newUrlsIteration.closeWith(newUrls);

            // Save off parsed page content. So just extract the parsed content piece of the Tuple2, and
            // then pass it on to the provided content sink function.
            outlinksOrContent.select("content")
            	.map(new MapFunction<Tuple2<ExtractedUrl, ParsedUrl>, ParsedUrl>() {

	                @Override
	                public ParsedUrl map(Tuple2<ExtractedUrl, ParsedUrl> in) throws Exception {
	                    return in.f1;
	                }
	            })
	            .name("Select fetched content")
            	.addSink(_contentSink)
            	.name("ContentSink");

            return new CrawlTopology(_env, _jobName);
        }

    }
}
