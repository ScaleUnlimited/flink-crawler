package com.scaleunlimited.flinkcrawler.tools;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.scaleunlimited.flinkcrawler.crawldb.InMemoryCrawlDB;
import com.scaleunlimited.flinkcrawler.parser.SimplePageParser;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.sources.BaseUrlSource;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.tools.CrawlTopology.CrawlTopologyBuilder;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlLengthener;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;

import crawlercommons.robots.SimpleRobotRulesParser;
import crawlercommons.fetcher.http.SimpleHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

public class CrawlTool {

	static class CrawlToolOptions {
	    private String _urlsFilename;

		@Option(name = "-seedurls", usage = "text file containing list of seed urls", required = true)
	    public void setSeedUrlsFilename(String urlsFilename) {
	        _urlsFilename = urlsFilename;
	    }
	
		public String getSeedUrlsFilename() {
			return _urlsFilename;
		}
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
        
        
        // TODO For now assume seedUrlsFile is a local file - this needs to change to handle 
        // other file systems in the future.
        File seedUrlsFile = new File(options.getSeedUrlsFilename());
		if (!seedUrlsFile.exists()) {
			throw new RuntimeException("Seed urls file doesn't exist :" + seedUrlsFile.getAbsolutePath());
		}
		
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			
			
			CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env)
				.setUrlSource(createSeedUrlSource(seedUrlsFile))
				.setCrawlDB(new InMemoryCrawlDB())
				.setUrlLengthener(new SimpleUrlLengthener())
				.setRobotsFetcher(new SimpleHttpFetcher(new UserAgent("bogus", "bogus@domain.com", "http://domain.com")))
				.setRobotsParser(new SimpleRobotRulesParser())
				.setPageParser(new SimplePageParser())
				.setContentSink(new DiscardingSink<ParsedUrl>())
				.setUrlNormalizer(new SimpleUrlNormalizer())
				.setUrlFilter(new SimpleUrlValidator())
				.setPageFetcher(new SimpleHttpFetcher(new UserAgent("bogus", "bogus@domain.com", "http://domain.com")));
			
			builder.build().execute();
		} catch (Throwable t) {
			System.err.println("Error running CrawlTool: " + t.getMessage());
			t.printStackTrace(System.err);
			System.exit(-1);
		}
	}


	private static BaseUrlSource createSeedUrlSource(File seedUrlsFile) {
		try {
			// TODO Need to handle trim, skip commented lines, etc
			List<String> rawUrls = FileUtils.readLines(seedUrlsFile);
			return new SeedUrlSource(1.0f, rawUrls.toArray(new String[rawUrls.size()]));
		} catch (Exception e) {
			throw new RuntimeException("Unexpected error reading seed urls file :", e);
		}
	}

}
