package com.scaleunlimited.flinkcrawler.tools;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.scaleunlimited.flinkcrawler.config.SimpleHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.crawldb.InMemoryCrawlDB;
import com.scaleunlimited.flinkcrawler.parser.SimplePageParser;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.tools.CrawlTopology.CrawlTopologyBuilder;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;

import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.robots.SimpleRobotRulesParser;
import crawlercommons.url.PaidLevelDomain;

public class CrawlTool {

	static class CrawlToolOptions {
	    private String _urlsFilename;
	    private String _singleDomain;

		@Option(name = "-seedurls", usage = "text file containing list of seed urls", required = true)
	    public void setSeedUrlsFilename(String urlsFilename) {
	        _urlsFilename = urlsFilename;
	    }
		
		@Option(name = "-singledomain", usage = "only fetch URLs within this domain (and its sub-domains)", required = false)
	    public void setSingleDomain(String urlsFilename) {
			_singleDomain = urlsFilename;
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
			
			CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env)
				.setUrlSource(new SeedUrlSource(1.0f, options.getSeedUrlsFilename()))
				.setCrawlDB(new InMemoryCrawlDB())
				.setUrlLengthener(new SimpleUrlLengthener())
				.setRobotsFetcherBuilder(new SimpleHttpFetcherBuilder(userAgent))
				.setRobotsParser(new SimpleRobotRulesParser())
				.setPageParser(new SimplePageParser())
				.setContentSink(new DiscardingSink<ParsedUrl>())
				.setUrlNormalizer(new SimpleUrlNormalizer())
				.setUrlFilter(urlValidator)
				.setPageFetcherBuilder(new SimpleHttpFetcherBuilder(userAgent));
			
			builder.build().execute();
		} catch (Throwable t) {
			System.err.println("Error running CrawlTool: " + t.getMessage());
			t.printStackTrace(System.err);
			System.exit(-1);
		}
	}

}
