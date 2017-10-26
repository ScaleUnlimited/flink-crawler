package com.scaleunlimited.flinkcrawler.tools;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironmentWithAsyncExecution;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB;
import com.scaleunlimited.flinkcrawler.crawldb.DefaultCrawlDBMerger;
import com.scaleunlimited.flinkcrawler.crawldb.InMemoryCrawlDB;
import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.SimpleHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.functions.CheckUrlWithRobotsFunction;
import com.scaleunlimited.flinkcrawler.functions.CrawlDBFunction;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.HandleFailedSiteMapFunction;
import com.scaleunlimited.flinkcrawler.functions.HashPartitioner;
import com.scaleunlimited.flinkcrawler.functions.LengthenUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.NormalizeUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.OutlinkToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseSiteMapFunction;
import com.scaleunlimited.flinkcrawler.functions.PldKeySelector;
import com.scaleunlimited.flinkcrawler.functions.UrlKeySelector;
import com.scaleunlimited.flinkcrawler.functions.ValidUrlsFilter;
import com.scaleunlimited.flinkcrawler.parser.BasePageParser;
import com.scaleunlimited.flinkcrawler.parser.SimplePageParser;
import com.scaleunlimited.flinkcrawler.parser.SimpleSiteMapParser;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.BaseUrlSource;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.sources.TicklerSource;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlValidator;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.FlinkUtils;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.robots.SimpleRobotRulesParser;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;

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
    private JobID _jobID;
    
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

    public JobSubmissionResult executeAsync() throws Exception {
    	if (!(_env instanceof LocalStreamEnvironmentWithAsyncExecution)) {
    		throw new IllegalStateException("StreamExecutionEnvironment must be LocalStreamEnvironmentWithAsyncExecution for async execution");
    	}
    	
    	LocalStreamEnvironmentWithAsyncExecution env = (LocalStreamEnvironmentWithAsyncExecution)_env;
    	JobSubmissionResult result = env.executeAsync(_jobName);
    	_jobID = result.getJobID();
    	return result;
    }
    
    public boolean isRunning() throws Exception {
    	if (!(_env instanceof LocalStreamEnvironmentWithAsyncExecution)) {
    		throw new IllegalStateException("StreamExecutionEnvironment must be LocalStreamEnvironmentWithAsyncExecution for async execution");
    	}
    	
    	LocalStreamEnvironmentWithAsyncExecution env = (LocalStreamEnvironmentWithAsyncExecution)_env;
    	return env.isRunning(_jobID);
    }
    
    public void stop() throws Exception {
    	if (!(_env instanceof LocalStreamEnvironmentWithAsyncExecution)) {
    		throw new IllegalStateException("StreamExecutionEnvironment must be LocalStreamEnvironmentWithAsyncExecution for async execution");
    	}
    	
    	LocalStreamEnvironmentWithAsyncExecution env = (LocalStreamEnvironmentWithAsyncExecution)_env;
    	env.stop(_jobID);
    	
    	// Stop the job execution environment.
    	env.stop();
    }
    
    public static class CrawlTopologyBuilder {

		private static final UserAgent INVALID_USER_AGENT = 
			new UserAgent(	"DO NOT USE THIS USER AGENT (i.e., MAKE YOUR OWN)!", 
							"flink-crawler@scaleunlimited.com", 
							"https://github.com/ScaleUnlimited/flink-crawler/wiki/Crawler-Policy");

		public static final int DEFAULT_PARALLELISM = -1;

        private StreamExecutionEnvironment _env;
        private String _jobName = "flink-crawler";
        private int _parallelism = DEFAULT_PARALLELISM;
        private long _forceCrawlDelay = CrawlTool.DO_NOT_FORCE_CRAWL_DELAY;
        private long _defaultCrawlDelay = 10_000L;
        
        private BaseUrlSource _urlSource = new SeedUrlSource(makeDefaultSeedUrl());;
        private FetchQueue _fetchQueue = new FetchQueue(10_000);
        private BaseCrawlDB _crawlDB = new InMemoryCrawlDB();
        
        private int _maxFetcherPoolSize = FetchUrlsFunction.DEFAULT_THREAD_COUNT;
        private BaseHttpFetcherBuilder _robotsFetcherBuilder = new SimpleHttpFetcherBuilder(INVALID_USER_AGENT);
        private SimpleRobotRulesParser _robotsParser = new SimpleRobotRulesParser();

        private BaseUrlLengthener _urlLengthener = new SimpleUrlLengthener();
        private SinkFunction<ParsedUrl> _contentSink = new DiscardingSink<ParsedUrl>();
        private SinkFunction<String> _contentTextSink;
        private String _contentTextFilePathString;
        private BaseUrlNormalizer _urlNormalizer = new SimpleUrlNormalizer();
        private BaseUrlValidator _urlFilter = new SimpleUrlValidator();
        private BaseHttpFetcherBuilder _pageFetcherBuilder = new SimpleHttpFetcherBuilder(INVALID_USER_AGENT);
        private BaseHttpFetcherBuilder _siteMapFetcherBuilder = new SimpleHttpFetcherBuilder(INVALID_USER_AGENT);
        private BasePageParser _pageParser = new SimplePageParser();
		private BasePageParser _siteMapParser = new SimpleSiteMapParser();

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

        public CrawlTopologyBuilder setUrlSource(BaseUrlSource urlSource) {
            _urlSource = urlSource;
            return this;
        }
        
        public CrawlTopologyBuilder setMaxFetcherPoolSize(int maxPoolSize) {
        	_maxFetcherPoolSize = maxPoolSize;
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
        
        public CrawlTopologyBuilder setUserAgent(UserAgent userAgent) {
        	_pageFetcherBuilder.setUserAgent(userAgent);
        	_siteMapFetcherBuilder.setUserAgent(userAgent);
        	_robotsFetcherBuilder.setUserAgent(userAgent);
        	return this;
        }

        public CrawlTopologyBuilder setPageFetcherBuilder(BaseHttpFetcherBuilder pageFetcherBuilder) {
            _pageFetcherBuilder = pageFetcherBuilder;
            return this;
        }

        public CrawlTopologyBuilder setSiteMapFetcherBuilder(BaseHttpFetcherBuilder siteMapFetcherBuilder) {
            _siteMapFetcherBuilder = siteMapFetcherBuilder;
            return this;
        }

        public CrawlTopologyBuilder setPageParser(BasePageParser pageParser) {
            _pageParser = pageParser;
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

        public CrawlTopologyBuilder setContentSink(SinkFunction<ParsedUrl> contentSink) {
            _contentSink = contentSink;
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

        @SuppressWarnings("serial")
        public CrawlTopology build() {
            if (_parallelism != DEFAULT_PARALLELISM) {
            	_env.setParallelism(_parallelism);
            }
            
            if	(	(_robotsFetcherBuilder.getUserAgent() == INVALID_USER_AGENT)
        		||	(_siteMapFetcherBuilder.getUserAgent() == INVALID_USER_AGENT)
            	||	(_pageFetcherBuilder.getUserAgent() == INVALID_USER_AGENT)) {
            	throw new IllegalArgumentException("You must define your own UserAgent!");
            }
            
            // The Headers class in http-fetcher uses an unmodifiable list, which Kryo can't handle
            // unless we register a special serializer provided by the "kryo-serializers" project.
            _env.registerTypeWithKryoSerializer(Collections.unmodifiableList(new ArrayList<>()).getClass(), UnmodifiableCollectionsSerializer.class);
            
            DataStream<RawUrl> seedUrls = _env.addSource(_urlSource).name("Seed urls source");
            DataStream<CrawlStateUrl> cleanedUrls = cleanUrls(seedUrls);
            
            // Update the Crawl DB, then run URLs it emits through robots filtering.
            IterativeStream<CrawlStateUrl> crawlDbIteration = cleanedUrls.iterate(/* TicklerSource.TICKLE_INTERVAL * 5 */);
            DataStream<FetchUrl> preRobotsUrls = crawlDbIteration
            		.partitionCustom(new HashPartitioner(), new PldKeySelector<CrawlStateUrl>())
            		.flatMap(new CrawlDBFunction(_crawlDB, new DefaultCrawlDBMerger(), _fetchQueue))
            		.name("CrawlDBFunction")
            		// TODO still use KeyedStream here?
            		.partitionCustom(new HashPartitioner(), new PldKeySelector<FetchUrl>());
            
            DataStream<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> postRobotsUrls =
            		AsyncDataStream.unorderedWait(preRobotsUrls,
            				new CheckUrlWithRobotsFunction(_robotsFetcherBuilder, _robotsParser, _forceCrawlDelay, _defaultCrawlDelay),
            				_robotsFetcherBuilder.getTimeoutInSeconds(), TimeUnit.SECONDS)
                    .name("CheckUrlWithRobotsFunction");
            
            // Split this stream into passed, blocked or sitemap.
            SplitStream<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>> blockedOrPassedOrSitemapUrls = postRobotsUrls
            		.split(new OutputSelector<Tuple3<CrawlStateUrl, FetchUrl, FetchUrl>>() {
                        
            			private final List<String> BLOCKED_STREAM = Arrays.asList("blocked");
                        private final List<String> PASSED_STREAM = Arrays.asList("passed");
                        private final List<String> SITEMAP_STREAM = Arrays.asList("sitemap");

                        @Override
                        public Iterable<String> select(Tuple3<CrawlStateUrl, FetchUrl, FetchUrl> blockedOrPassedOrSitemapUrl) {
                            if (blockedOrPassedOrSitemapUrl.f0 != null) {
                                return BLOCKED_STREAM;
                            } else if (blockedOrPassedOrSitemapUrl.f1 != null) {
                                return PASSED_STREAM;
                            } else if (blockedOrPassedOrSitemapUrl.f2 != null) {
                                return SITEMAP_STREAM;
                            } else {
                                throw new RuntimeException("Invalid case of neither blocked nor passed nor sitemap");
                            }
                        }
            		});
            
            // Split off the sitemap urls and fetch and later parse them using the sitemap fetcher
            // and parser to generate outlinks
            KeyedStream<FetchUrl, Integer> sitemapUrlsToFetch = blockedOrPassedOrSitemapUrls.select("sitemap")
            		.map(new MapFunction<Tuple3<CrawlStateUrl,FetchUrl, FetchUrl>, FetchUrl>() {

						@Override
						public FetchUrl map(Tuple3<CrawlStateUrl, FetchUrl, FetchUrl> sitemapUrl)
								throws Exception {
							return sitemapUrl.f2;
						}
					})
					.name("Select sitemap URLs")
					.keyBy(new PldKeySelector<FetchUrl>());

            DataStream<Tuple2<CrawlStateUrl, FetchedUrl>> sitemapUrls = 
            		// TODO get capacity from fetcher builder.
            		AsyncDataStream.unorderedWait(sitemapUrlsToFetch, new FetchUrlsFunction(_siteMapFetcherBuilder, _maxFetcherPoolSize), _siteMapFetcherBuilder.getTimeoutInSeconds(), TimeUnit.SECONDS, 10000)
					.name("FetchUrlsFunction for sitemap"); // FUTURE Have a separate FetchSiteMapUrlFunction that extends FetchUrlsFunction
           
            // Run the failed urls into a custom function to log it and then to a DiscardingSink.
            // FUTURE - flag as sitemap and emit as any other url from the robots code; but this would require us to payload the flag through
            SplitStream<Tuple2<CrawlStateUrl, FetchedUrl>> siteMapFetchAttemptedUrls = splitFetchedUrlsStream(sitemapUrls);
            selectFetchStatus(siteMapFetchAttemptedUrls)
            		.filter(new HandleFailedSiteMapFunction())
            		.name("HandleFailedSiteMapFunction")
            		.addSink(new DiscardingSink<CrawlStateUrl>());
            
            DataStream<Tuple3<ExtractedUrl, ParsedUrl, String>> parsedSiteMapUrls = selectFetchedUrls(siteMapFetchAttemptedUrls)
					.flatMap(new ParseSiteMapFunction(_siteMapParser))
					.name("ParseSiteMapFunction");
            SplitStream<Tuple3<ExtractedUrl, ParsedUrl, String>> sitemapOutlinksContent = splitOutlinkContent(parsedSiteMapUrls);
            DataStream<RawUrl> newSiteMapExtractedUrls = sitemapOutlinksContent.select("outlink")
            		.map(new OutlinkToStateUrlFunction())
            		.name("OutlinkToStateUrlFunction");

            // Split off rejected URLs. These will get unioned (merged) with the status of URLs that we
            // attempt to fetch, and then fed back into the crawl DB via the inner iteration.
            DataStream<CrawlStateUrl> robotBlockedUrls = blockedOrPassedOrSitemapUrls.select("blocked")
            		.map(new MapFunction<Tuple3<CrawlStateUrl,FetchUrl, FetchUrl>, CrawlStateUrl>() {

						@Override
						public CrawlStateUrl map(Tuple3<CrawlStateUrl, FetchUrl, FetchUrl> blockedUrl)
								throws Exception {
							return blockedUrl.f0;
						}
					})
					.name("Select blocked URLs");
            
            // Fetch the URLs that passed our robots filter
            KeyedStream<FetchUrl, Integer> urlsToFetch = blockedOrPassedOrSitemapUrls.select("passed")
            		.map(new MapFunction<Tuple3<CrawlStateUrl,FetchUrl, FetchUrl>, FetchUrl>() {

						@Override
						public FetchUrl map(Tuple3<CrawlStateUrl, FetchUrl, FetchUrl> justHasFetchUrl)
								throws Exception {
							return justHasFetchUrl.f1;
						}
					})
					.name("Select passed URLs")
            		.keyBy(new PldKeySelector<FetchUrl>());
            		
            DataStream<Tuple2<CrawlStateUrl, FetchedUrl>> fetchedUrls = 
            		// TODO get capacity from fetcher builder.
            		AsyncDataStream.unorderedWait(urlsToFetch, new FetchUrlsFunction(_pageFetcherBuilder, _maxFetcherPoolSize), _pageFetcherBuilder.getTimeoutInSeconds(), TimeUnit.SECONDS, 10000)
                    .name("FetchUrlsFunction");
            
            SplitStream<Tuple2<CrawlStateUrl, FetchedUrl>> fetchAttemptedUrls = splitFetchedUrlsStream(fetchedUrls);
			// Get the status of all URLs we've attempted to fetch, so that we can  union them with URLs blocked by robots, 
            // and iterate those back into the crawl DB.
            DataStream<CrawlStateUrl> fetchStatusUrls = selectFetchStatus(fetchAttemptedUrls);

            DataStream<Tuple3<ExtractedUrl, ParsedUrl, String>> parsedUrls = selectFetchedUrls(fetchAttemptedUrls)
            														.flatMap(new ParseFunction(_pageParser))
            														.name("ParseFunction");
            

            SplitStream<Tuple3<ExtractedUrl, ParsedUrl, String>> outlinksOrContent = splitOutlinkContent(parsedUrls);

            DataStream<RawUrl> newRawUrls = outlinksOrContent.select("outlink")
            		.map(new OutlinkToStateUrlFunction())
            		.name("OutlinkToStateUrlFunction")
            		.union(newSiteMapExtractedUrls);
            		
            DataStream<CrawlStateUrl> newUrls = cleanUrls(newRawUrls);

            // We need to merge robotBlockedUrls with the "status" stream from fetchAttemptedUrls and status 
            // stream of the siteMapFetchAttemptedUrls and all of the new URLs from outlinks and sitemaps.
            // We also merge in tickler URLs, which keep the crawlDB alive.
            DataStream<CrawlStateUrl> tickler = _env.addSource(new TicklerSource())
            		.name("Tickler source");
            crawlDbIteration.closeWith(robotBlockedUrls.union(fetchStatusUrls, newUrls, tickler));
            
            // Save off parsed page content. So just extract the parsed content piece of the Tuple3, and
            // then pass it on to the provided content sink function.
            outlinksOrContent.select("content")
            	.map(new MapFunction<Tuple3<ExtractedUrl, ParsedUrl, String>, ParsedUrl>() {

	                @Override
	                public ParsedUrl map(Tuple3<ExtractedUrl, ParsedUrl, String> in) throws Exception {
	                    return in.f1;
	                }
	            })
	            .name("Select fetched content")
            	.addSink(_contentSink)
            	.name("ContentSink");

            // Save off parsed page content text. So just extract the parsed content text piece of the Tuple3, and
            // then pass it on to the provided content sink function (or just send it to the console).
            DataStream<String> contentText = outlinksOrContent.select("content_text")
            	.map(new MapFunction<Tuple3<ExtractedUrl, ParsedUrl, String>, String>() {

	                @Override
	                public String map(Tuple3<ExtractedUrl, ParsedUrl, String> in) throws Exception {
	                    return in.f2;
	                }
	            })
	            .name("Select fetched content text")
            	.name("ContentTextSink");
            if (_contentTextSink != null) {
            	contentText.addSink(_contentTextSink);
            } else if (_contentTextFilePathString != null) {
            	contentText.writeAsText(_contentTextFilePathString, WriteMode.OVERWRITE);
            } else {
            	contentText.print();
            }

            return new CrawlTopology(_env, _jobName);
        }


		/**
		 * Create a snippet of the topology that takes a RawUrl DataStream and applies an async
		 * lengthener, then a normalizer and a validator. What we get out is a CrawlStateUrl
		 * DataStream.
		 * 
		 * @param rawUrls
		 * @return
		 */
		private DataStream<CrawlStateUrl> cleanUrls(DataStream<RawUrl> rawUrls) {
    		// TODO LengthenUrlsFunction needs to just pass along invalid URLs
			rawUrls = rawUrls.keyBy(new UrlKeySelector<RawUrl>());
			return AsyncDataStream.unorderedWait(	rawUrls,
													new LengthenUrlsFunction(_urlLengthener),
												    _urlLengthener.getTimeoutInSeconds(),
												    TimeUnit.SECONDS)
					.name("LengthenUrlsFunction")
					.flatMap(new NormalizeUrlsFunction(_urlNormalizer))
					.name("NormalizeUrlsFunction")
					.flatMap(new ValidUrlsFilter(_urlFilter))
					.name("ValidUrlsFilter");
		}

		@SuppressWarnings("serial")
		private SplitStream<Tuple2<CrawlStateUrl, FetchedUrl>> splitFetchedUrlsStream(DataStream<Tuple2<CrawlStateUrl, FetchedUrl>> fetchedUrls) {
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
			return fetchAttemptedUrls;
		}
		
		@SuppressWarnings("serial")
		private DataStream<CrawlStateUrl> selectFetchStatus(SplitStream<Tuple2<CrawlStateUrl, FetchedUrl>> fetchAttemptedUrls) {
            DataStream<CrawlStateUrl> fetchStatusUrls = fetchAttemptedUrls.select("fetch_status")
            		.map(new MapFunction<Tuple2<CrawlStateUrl, FetchedUrl>, CrawlStateUrl>() {

						@Override
						public CrawlStateUrl map(Tuple2<CrawlStateUrl, FetchedUrl> url) throws Exception {
							return url.f0;
						}
					})
					.name("Select fetch status");
			return fetchStatusUrls;
		}

		@SuppressWarnings("serial")
		private DataStream<FetchedUrl> selectFetchedUrls(SplitStream<Tuple2<CrawlStateUrl, FetchedUrl>> fetchAttemptedUrls) {
            DataStream<FetchedUrl> fetchedUrls = fetchAttemptedUrls.select("fetched_url")
            		.map(new MapFunction<Tuple2<CrawlStateUrl, FetchedUrl>, FetchedUrl>() {

						@Override
						public FetchedUrl map( Tuple2<CrawlStateUrl, FetchedUrl> hasFetchedUrl)
								throws Exception {
							return hasFetchedUrl.f1;
						}
					})
					.name("Select fetched URLs");
			return fetchedUrls;
		}

		@SuppressWarnings("serial")
		private SplitStream<Tuple3<ExtractedUrl, ParsedUrl, String>> splitOutlinkContent(DataStream<Tuple3<ExtractedUrl, ParsedUrl, String>> parsedUrls) {
			SplitStream<Tuple3<ExtractedUrl, ParsedUrl, String>> outlinksOrContent = parsedUrls
                    .split(new OutputSelector<Tuple3<ExtractedUrl, ParsedUrl, String>>() {

                        private final List<String> OUTLINK_STREAM = Arrays.asList("outlink");
                        private final List<String> CONTENT_STREAM = Arrays.asList("content");
                        private final List<String> CONTENT_TEXT_STREAM = Arrays.asList("content_text");

                        @Override
                        public Iterable<String> select(Tuple3<ExtractedUrl, ParsedUrl, String> outlinksOrContent) {
                            if (outlinksOrContent.f0 != null) {
                                return OUTLINK_STREAM;
                            } else if (outlinksOrContent.f1 != null) {
                                return CONTENT_STREAM;
                            } else if (outlinksOrContent.f2 != null) {
                                return CONTENT_TEXT_STREAM;
                            } else {
                                throw new RuntimeException("Invalid case of neither outlink, content, nor content_text");
                            }
                        }
                    });
			return outlinksOrContent;
		}
    }
    
	private static RawUrl makeDefaultSeedUrl() {
		try {
			return new RawUrl("http://www.scaleunlimited.com/");
		} catch (MalformedURLException e) {
			throw new RuntimeException("URL should parse just fine?");
		}
	}

	/**
	 * Trigger async execution, and then monitor the job. Fail if it the job is
	 * still running after <maxDurationMS> milliseconds.
	 * 
	 * @param maxDurationMS Maximum allowable execution time.
	 * @param maxQuietTimeMS Length of time w/no recorded activity after which we'll terminate.
	 * @throws Exception
	 */
	public void execute(int maxDurationMS, int maxQuietTimeMS) throws Exception {
		executeAsync();
		
		boolean terminated = false;
		long endTime = System.currentTimeMillis() + maxDurationMS;
		while (System.currentTimeMillis() < endTime) {
			long lastActivityTime = UrlLogger.getLastActivityTime();
			if (lastActivityTime != UrlLogger.NO_ACTIVITY_TIME) {
				long curTime = System.currentTimeMillis();
				if ((curTime - lastActivityTime) > maxQuietTimeMS) {
					stop();
					terminated = true;
					break;
				}
			}

			Thread.sleep(100L);
		}
		
		if (!terminated) {
			stop();
			throw new RuntimeException("Job did not terminate in time");
		}
	}
}
