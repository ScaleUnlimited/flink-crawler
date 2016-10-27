package com.scaleunlimited.flinkcrawler.tools;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import com.scaleunlimited.flinkcrawler.functions.CheckUrlWithRobotsFunction;
import com.scaleunlimited.flinkcrawler.functions.CrawlDBFunction;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseFunction;
import com.scaleunlimited.flinkcrawler.parser.SimpleParser;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.tools.CrawlTopology.CrawlTopologyBuilder;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlValidator;

public class CrawlTool {

	public static void main(String[] args) {
		
		// Generate topology, run it
		
		try {
			LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
			
			CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env)
				.setCrawlDBFunction(new CrawlDBFunction())
				.setFetchFunction(new FetchUrlsFunction())
				.setRobotsFunction(new CheckUrlWithRobotsFunction())
				.setParseFunction(new ParseFunction(new SimpleParser()))
				.setContentSink(new DiscardingSink<ParsedUrl>())
				.setUrlNormalizer(new SimpleUrlNormalizer())
				.setUrlFilter(new SimpleUrlValidator())
				.setRunTime(1000);
			
			builder.build().execute();
		} catch (Throwable t) {
			System.err.println("Error running CrawlTool: " + t.getMessage());
			t.printStackTrace(System.err);
			System.exit(-1);
		}
	}

}
