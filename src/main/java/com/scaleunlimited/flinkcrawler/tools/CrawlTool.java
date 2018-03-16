package com.scaleunlimited.flinkcrawler.tools;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpStatus;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.SimpleHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.commoncrawl.CommonCrawlFetcherBuilder;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.topology.CrawlTopologyBuilder;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;
import com.scaleunlimited.flinkcrawler.urls.SingleDomainUrlValidator;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.SimpleHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.util.Headers;

public class CrawlTool {

	public static final long DO_NOT_FORCE_CRAWL_DELAY = -1L;
	
	// As per https://developers.google.com/search/reference/robots_txt
    private static final int MAX_ROBOTS_TXT_SIZE = 500 * 1024;

	public static class CrawlToolOptions {
		
	    public static final int DEFAULT_MAX_OUTLINKS_PER_PAGE = 50;

	    private UserAgent _userAgent = null;
		private String _urlsFilename;
	    private String _singleDomain;
        private long _forceCrawlDelay = CrawlTool.DO_NOT_FORCE_CRAWL_DELAY;
        private long _defaultCrawlDelayMS = 10 * 1000L;
        private int _maxContentSize = SimpleHttpFetcher.DEFAULT_MAX_CONTENT_SIZE;
        private int _fetchersPerTask = 1;
        private int _parallelism = CrawlTopologyBuilder.DEFAULT_PARALLELISM;
        private String _outputFile = null;
        private boolean _htmlOnly = false;
        private int _crawlDbParallelism = 1;
        private String _checkpointDir = null;
        private int _maxOutlinksPerPage = DEFAULT_MAX_OUTLINKS_PER_PAGE;
        
        private String _cacheDir;
        private String _commonCrawlId = null;
        
        @Option(name = "-agent", usage = "user agent info, format:'name,email,website'", required = false)
        public void setUserAgent(String agentNameWebsiteEmailString) {
            if (isCommonCrawl()) {
                throw new RuntimeException("user agent not used in common crawl mode");
            }
            String fields[] = agentNameWebsiteEmailString.split(",", 4);
            if (fields.length != 3) {
                throw new RuntimeException("    Invalid format for user agent (expected 'name,email,website'): "
                                            +   agentNameWebsiteEmailString);
            }
            String agentName = fields[0];
            String agentEmail = fields[1];
            String agentWebSite = fields[2];
            if (!(agentEmail.contains("@"))) {
                throw new RuntimeException("Invalid email address for user agent: " + agentEmail);
            }
            if (!(agentWebSite.startsWith("http"))) {
                throw new RuntimeException("Invalid web site URL for user agent: " + agentWebSite);
            }
            setUserAgent(new UserAgent(agentName, agentEmail, agentWebSite));
        }
        
        public void setUserAgent(UserAgent newUserAgent) {
            _userAgent = newUserAgent;
        }
        
		@Option(name = "-seedurls", usage = "text file containing list of seed urls", required = true)
	    public void setSeedUrlsFilename(String urlsFilename) {
	        _urlsFilename = urlsFilename;
	    }
		
        @Option(name = "-singledomain", usage = "only fetch URLs within this domain (and its sub-domains)", required = false)
        public void setSingleDomain(String singleDomain) {
            _singleDomain = singleDomain;
        }
		
		@Option(name = "-forcecrawldelay", usage = "use this crawl delay (ms) even if robots.txt provides something else", required = false)
	    public void setForceCrawlDelay(long forceCrawlDelay) {
			_forceCrawlDelay = forceCrawlDelay;
	    }
		
		@Option(name = "-defaultcrawldelay", usage = "use this crawl delay (ms) when robots.txt doesn't provide it", required = false)
	    public void setDefaultCrawlDelayMS(long defaultCrawlDelayMS) {
			_defaultCrawlDelayMS = defaultCrawlDelayMS;
	    }
		
        @Option(name = "-maxcontentsize", usage = "maximum content size", required = false)
        public void setMaxContentSize(int maxContentSize) {
            _maxContentSize = maxContentSize;
        }
        
        @Option(name = "-crawldbparallelism", usage = "parallelism for crawl DB", required = false)
        public void setCrawlDbParallelism(int parallelism) {
            _crawlDbParallelism = parallelism;
        }
        
		@Option(name = "-commoncrawl", usage = "crawl id for CommonCrawl.org dataset", required = false)
	    public void setCommonCrawlId(String commonCrawlId) {
			_commonCrawlId = commonCrawlId;
	    }
		
		@Option(name = "-cachedir", usage = "cache location for CommonCrawl.org secondary index", required = false)
	    public void setCommonCrawlCacheDir(String cacheDir) {
			_cacheDir = cacheDir;
	    }
		
		@Option(name = "-fetcherspertask", usage = "fetchers per task", required = false)
	    public void setFetchersPerTask(int fetchersPerTask) {
			_fetchersPerTask = fetchersPerTask;
	    }
		
		@Option(name = "-parallelism", usage = "Flink paralellism", required = false)
	    public void setParallelism(int parallelism) {
			_parallelism = parallelism;
	    }
		
        @Option(name = "-outputfile", usage = "Local file to store fetched content (testing only)", required = false)
        public void setOutputFile(String outputFile) {
            _outputFile = outputFile;
        }
        
        @Option(name = "-checkpointdir", usage = "URI to directory to store checkpoint (enables checkpointing)", required = false)
        public void setCheckpointDir(String checkpointDir) {
            _checkpointDir = checkpointDir;
        }
		
		@Option(name = "-htmlonly", usage = "Only (fully) fetch and parse HTML pages", required = false)
	    public void setHtmlOnly(boolean htmlOnly) {
			_htmlOnly = htmlOnly;
	    }
		
        @Option(name = "-maxoutlinks", usage = "maximum coutlinks per page that are extracted (default:50)", required = false)
        public void setMaxOutlinksPerPage(int maxOutlinksPerPage) {
            _maxOutlinksPerPage = maxOutlinksPerPage;
        }
		
		public void validate() {
		    if (_commonCrawlId == null) {
                if (_userAgent == null) {
                    throw new RuntimeException("-agent is required (except for common crawl mode)");
                }
		    } else {
                if (_userAgent != null) {
                    throw new RuntimeException("user agent not used in common crawl mode");
                }
		    }
		}
		
		
		public UserAgent getUserAgent() {
		    validate();
            return _userAgent;
        }

        public String getSeedUrlsFilename() {
            validate();
			return _urlsFilename;
		}
		
		public boolean isSingleDomain() {
            validate();
			return (_singleDomain != null);
		}
		
		public String getSingleDomain() {
            validate();
			return _singleDomain;
		}
		
		public long getForceCrawlDelay() {
            validate();
			return _forceCrawlDelay;
		}

		public long getDefaultCrawlDelay() {
            validate();
			return _defaultCrawlDelayMS;
		}

		public int getMaxContentSize() {
            validate();
			return _maxContentSize;
		}
		
		public int getCrawlDbParallelism() {
		    return _crawlDbParallelism;
		}
		
		public boolean isCommonCrawl() {
            validate();
			return _commonCrawlId != null;
		}
		
		public String getCommonCrawlId() {
            validate();
			return _commonCrawlId;
		}
		
		public String getCommonCrawlCacheDir() {
            validate();
			return _cacheDir;
		}
		
		public int getFetchersPerTask() {
            validate();
			return _fetchersPerTask;
		}
		
		public int getParallelism() {
            validate();
			return _parallelism;
		}
		
		public String getOutputFile() {
            validate();
			return _outputFile;
		}
		
        public String getCheckpointDir() {
            validate();
            return _checkpointDir;
        }
        
		public boolean isHtmlOnly() {
            validate();
			return _htmlOnly;
		}

        public int getMaxOutlinksPerPage() {
            return _maxOutlinksPerPage;
        }
	}
	
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }
	
	public static void main(String[] args) {
		
        // Dump the classpath to stdout to debug artifact version conflicts
//		System.out.println(    "Java classpath: "
//                    		+   System.getProperty("java.class.path", "."));

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
			// Not really needed, as we are limited by fetch time and parsing CPU
			// env.getConfig().enableObjectReuse();
			
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

	public static void run(StreamExecutionEnvironment env, CrawlToolOptions options) throws Exception {
		
	    // TODO Complain if -cachedir is specified when not running locally?
	    
	    SimpleUrlValidator urlValidator =
			(	options.isSingleDomain() ?
				new SingleDomainUrlValidator(options.getSingleDomain())
			:	new SimpleUrlValidator());
		
		UserAgent userAgent =
	        (   options.isCommonCrawl() ?
                new UserAgent("unused-common-crawl-user-agent", "", "")
            :   options.getUserAgent());

		SimpleUrlLengthener urlLengthener = getUrlLengthener(options, userAgent);
		BaseHttpFetcherBuilder siteMapFetcherBuilder = getPageFetcherBuilder(options, userAgent)
			.setDefaultMaxContentSize(SiteMapParser.MAX_BYTES_ALLOWED);
		BaseHttpFetcherBuilder robotsFetcherBuilder = getRobotsFetcherBuilder(options, userAgent)
			.setDefaultMaxContentSize(MAX_ROBOTS_TXT_SIZE);
		BaseHttpFetcherBuilder pageFetcherBuilder = getPageFetcherBuilder(options, userAgent)
			.setDefaultMaxContentSize(options.getMaxContentSize());
		
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
                .setUrlSource(new SeedUrlSource(options.getCrawlDbParallelism(),
                        options.getSeedUrlsFilename(), RawUrl.DEFAULT_SCORE))
                .setRobotsFetcherBuilder(robotsFetcherBuilder)
                .setUrlFilter(urlValidator)
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

    private static SimpleUrlLengthener getUrlLengthener(CrawlToolOptions options, UserAgent userAgent) {
        if (options.isCommonCrawl()) {
            return new CommonCrawlUrlLengthener(userAgent);
        }
        return new SimpleUrlLengthener(userAgent);
    }
    
    @SuppressWarnings("serial")
    private static class CommonCrawlUrlLengthener extends SimpleUrlLengthener {

        public CommonCrawlUrlLengthener(UserAgent userAgent) {
            super(userAgent);
        }

        @Override
        public void open() throws Exception {
            // We never even build the fetcher
        }

        @Override
        public RawUrl lengthen(RawUrl url) {
            // Never lengthen anything
            return url;
        }
    }
    
	private static BaseHttpFetcherBuilder getPageFetcherBuilder(CrawlToolOptions options, UserAgent userAgent) throws IOException {
		if (options.isCommonCrawl()) {
            return new CommonCrawlFetcherBuilder(   options.getFetchersPerTask(), 
			                                        userAgent, 
			                                        options.getCommonCrawlId(),
			                                        options.getCommonCrawlCacheDir());
		}
        return new SimpleHttpFetcherBuilder(options.getFetchersPerTask(), userAgent);
	}

	@SuppressWarnings("serial")
	private static BaseHttpFetcherBuilder getRobotsFetcherBuilder(CrawlToolOptions options, UserAgent userAgent) throws IOException {
		
		// Although the static Common Crawl data does have robots.txt files
		// (in a separate location), there's no index, so it would be ugly to
		// have to download the whole thing.  For now, let's just pretend that
		// nobody has a robots.txt file by using a fetcher that always returns
		// a 404.
		if (options.isCommonCrawl()) {
			return new BaseHttpFetcherBuilder(0, userAgent) {
				
				@Override
				public BaseHttpFetcher build() throws Exception {
					return new BaseHttpFetcher(0, userAgent) {
						
						@Override
						public FetchedResult get(String robotsUrl, Payload payload)
								throws BaseFetchException {
							final int responseRate = 1000;
							return new FetchedResult(	robotsUrl, 
														robotsUrl, 
														0, 
														new Headers(), 
														new byte[0], 
														"text/plain", 
														responseRate, 
														payload, 
														robotsUrl, 
														0, 
														"192.168.1.1", 
														HttpStatus.SC_NOT_FOUND, 
														null);
						}
						
						@Override
						public void abort() {
						}
					};
				}
			};
		}
        return new SimpleHttpFetcherBuilder(userAgent);
	}

}
