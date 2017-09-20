package com.scaleunlimited.flinkcrawler.tools;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.http.HttpStatus;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.scaleunlimited.flinkcrawler.crawldb.InMemoryCrawlDB;
import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.SimpleHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.fetcher.commoncrawl.CommonCrawlFetcherBuilder;
import com.scaleunlimited.flinkcrawler.parser.SimplePageParser;
import com.scaleunlimited.flinkcrawler.parser.SimpleSiteMapParser;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.tools.CrawlTopology.CrawlTopologyBuilder;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;

import crawlercommons.domains.PaidLevelDomain;
import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.SimpleHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.robots.SimpleRobotRulesParser;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.util.Headers;

public class CrawlTool {

	public static final long DO_NOT_FORCE_CRAWL_DELAY = -1L;

	public static class CrawlToolOptions {
		
		private String _urlsFilename;
	    private String _singleDomain;
        private long _forceCrawlDelay = CrawlTool.DO_NOT_FORCE_CRAWL_DELAY;
        private long _defaultCrawlDelay = 10 * 1000L;
        private int _maxContentSize = SimpleHttpFetcher.DEFAULT_MAX_CONTENT_SIZE;
        private long _maxWaitTime = 5000;
        private int _maxThreads = 1;
        private int _parallelism = CrawlTopologyBuilder.DEFAULT_PARALLELISM;
        private String _outputFile = null;
        
        private String _cacheDir;
        private String _commonCrawlId;
        
		@Option(name = "-seedurls", usage = "text file containing list of seed urls", required = true)
	    public void setSeedUrlsFilename(String urlsFilename) {
	        _urlsFilename = urlsFilename;
	    }
		
		@Option(name = "-singledomain", usage = "only fetch URLs within this domain (and its sub-domains)", required = false)
	    public void setSingleDomain(String urlsFilename) {
			_singleDomain = urlsFilename;
	    }
		
		@Option(name = "-forcecrawldelay", usage = "use this crawl delay (ms) even if robots.txt provides something else", required = false)
	    public void setForceCrawlDelay(long forceCrawlDelay) {
			_forceCrawlDelay = forceCrawlDelay;
	    }
		
		@Option(name = "-defaultcrawldelay", usage = "use this crawl delay (ms) when robots.txt doesn't provide it", required = false)
	    public void setDefaultCrawlDelay(long defaultCrawlDelay) {
			_defaultCrawlDelay = defaultCrawlDelay;
	    }
		
		@Option(name = "-maxcontentsize", usage = "maximum content size", required = false)
	    public void setMaxContentSize(int maxContentSize) {
			_maxContentSize = maxContentSize;
	    }
		
		@Option(name = "-maxwaittime", usage = "maximum idle time (ms) before flink job terminates", required = false)
	    public void setMaxWaitTime(long maxWaitTime) {
			_maxWaitTime = maxWaitTime;
	    }
		
		@Option(name = "-commoncrawl", usage = "crawl id for CommonCrawl.org dataset", required = false)
	    public void setCommonCrawlId(String commonCrawlId) {
			_commonCrawlId = commonCrawlId;
	    }
		
		@Option(name = "-cachedir", usage = "cache location for CommonCrawl.org secondary index", required = false)
	    public void setCommonCrawlCacheDir(String cacheDir) {
			_cacheDir = cacheDir;
	    }
		
		@Option(name = "-maxthreads", usage = "max threads per fetcher", required = false)
	    public void setMaxThreads(int maxThreads) {
			_maxThreads = maxThreads;
	    }
		
		@Option(name = "-parallelism", usage = "Flink paralellism", required = false)
	    public void setParallelism(int parallelism) {
			_parallelism = parallelism;
	    }
		
		@Option(name = "-outputfile", usage = "Local file to store fetched content (testing only)", required = false)
	    public void setOutputFile(String outputFile) {
			_outputFile = outputFile;
	    }
		
		
		public String getSeedUrlsFilename() {
			return _urlsFilename;
		}
		
		public boolean isSingleDomain() {
			return (_singleDomain != null);
		}
		
		public String getSingleDomain() {
			return _singleDomain;
		}
		
		public long getForceCrawlDelay() {
			return _forceCrawlDelay;
		}

		public long getDefaultCrawlDelay() {
			return _defaultCrawlDelay;
		}

		public int getMaxContentSize() {
			return _maxContentSize;
		}
		
		public long getMaxWaitTime() {
			return _maxWaitTime;
		}
		
		public boolean isCommonCrawl() {
			return _commonCrawlId != null;
		}
		
		public String getCommonCrawlId() {
			return _commonCrawlId;
		}
		
		public String getCommonCrawlCacheDir() {
			return _cacheDir;
		}
		
		public int getMaxThreads() {
			return _maxThreads;
		}
		
		public int getParallelism() {
			return _parallelism;
		}
		
		public String getOutputFile() {
			return _outputFile;
		}
	}
	
	@SuppressWarnings("serial")
	static class SingleDomainUrlValidator extends SimpleUrlValidator {
	    private String _singleDomain;
	    
		public SingleDomainUrlValidator(String singleDomain) {
			super();
			_singleDomain = singleDomain;
		}

		@Override
		public boolean isValid(String urlString) {
			if (!(super.isValid(urlString))) {
				return false;
			}
			return isUrlWithinDomain(urlString, _singleDomain);
		}
	}

    /**
     * Check whether the domain of the URL is the given domain or a subdomain
     * of the given domain.
     * 
     * @param url
     * @param domain
     * @return true iff url is "within" domain
     */
    public static boolean isUrlWithinDomain(String url, String domain) {
        try {
            for (   String urlDomain = new URL(url).getHost();
                    urlDomain != null;
                    urlDomain = getSuperDomain(urlDomain)) {
                if (urlDomain.equalsIgnoreCase(domain)) {
                    return true;
                }
            }
        } catch (MalformedURLException e) {
            return false;
        }
        
        return false;
    }
    
    /**
     * Extract the domain immediately containing this subdomain.
     * 
     * @param hostname 
     * @return immediate super domain of hostname, or null if hostname
     * is already a paid-level domain (i.e., not really a subdomain).
     */
    public static String getSuperDomain(String hostname) {
        String pld = PaidLevelDomain.getPLD(hostname);
        if (hostname.equalsIgnoreCase(pld)) {
            return null;
        }
        return hostname.substring(hostname.indexOf(".")+1);
    }
    
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }
	
	public static void main(String[] args) {
		
        // Dump the classpath to stdout to debug artifact version conflicts
		System.out.println(    "Java classpath: "
                    		+   System.getProperty("java.class.path", "."));

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
			
			UserAgent userAgent = new UserAgent("flink-crawler", "flink-crawler@scaleunlimited.com", "https://github.com/ScaleUnlimited/flink-crawler/wiki/Crawler-Policy");
			
			SimpleUrlValidator urlValidator =
				(	options.isSingleDomain() ?
					new SingleDomainUrlValidator(options.getSingleDomain())
				:	new SimpleUrlValidator());
			
			BaseHttpFetcherBuilder siteMapFetcherBuilder = getPageFetcherBuilder(options, userAgent)
				.setDefaultMaxContentSize(SiteMapParser.MAX_BYTES_ALLOWED);
			BaseHttpFetcherBuilder robotsFetcherBuilder = getRobotsFetcherBuilder(options, userAgent)
				.setDefaultMaxContentSize(options.getMaxContentSize());
			BaseHttpFetcherBuilder pageFetcherBuilder = getPageFetcherBuilder(options, userAgent)
					.setDefaultMaxContentSize(options.getMaxContentSize());
			CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env)
//				.setMaxWaitTime(100000)
				.setUrlSource(new SeedUrlSource(options.getSeedUrlsFilename(), 1.0f))
				.setCrawlDB(new InMemoryCrawlDB())
				.setUrlLengthener(new SimpleUrlLengthener())
				.setRobotsFetcherBuilder(robotsFetcherBuilder)
				.setRobotsParser(new SimpleRobotRulesParser())
				.setPageParser(new SimplePageParser())
				.setContentSink(new DiscardingSink<ParsedUrl>())
				.setUrlNormalizer(new SimpleUrlNormalizer())
				.setUrlFilter(urlValidator)
				.setSiteMapFetcherBuilder(siteMapFetcherBuilder)
				.setSiteMapParser(new SimpleSiteMapParser())
				.setPageFetcherBuilder(pageFetcherBuilder)
				.setForceCrawlDelay(options.getForceCrawlDelay())
				.setDefaultCrawlDelay(options.getDefaultCrawlDelay())
				.setMaxWaitTime(options.getMaxWaitTime())
				.setParallelism(options.getParallelism());
			
			if (options.getOutputFile() != null) {
				builder.setContentTextFile(options.getOutputFile());
			}
			
			builder.build().execute();
		} catch (Throwable t) {
			System.err.println("Error running CrawlTool: " + t.getMessage());
			t.printStackTrace(System.err);
			System.exit(-1);
		}
	}

	private static BaseHttpFetcherBuilder getPageFetcherBuilder(CrawlToolOptions options, UserAgent userAgent) throws IOException {
		if (options.isCommonCrawl()) {
			return new CommonCrawlFetcherBuilder(options.getMaxThreads(), userAgent)
					.setCrawlId(options.getCommonCrawlId())
					.setCacheDir(options.getCommonCrawlCacheDir())
					.prepCache();
		} else {
			return new SimpleHttpFetcherBuilder(userAgent);
		}
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
		} else {
			return new SimpleHttpFetcherBuilder(userAgent);
		}
	}

}
