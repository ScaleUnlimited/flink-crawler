package com.scaleunlimited.flinkcrawler.tools;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.scaleunlimited.flinkcrawler.functions.CheckUrlWithRobotsFunction;
import com.scaleunlimited.flinkcrawler.functions.CrawlDBFunction;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.tools.CrawlTopology.CrawlTopologyBuilder;

public class CrawlTool {

	public static void main(String[] args) {
		
		// Generate topology, run it
		
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
			
			CrawlTopologyBuilder builder = new CrawlTopologyBuilder(env)
				.setCrawlDBFunction(new CrawlDBFunction())
				.setFetchFunction(new FetchUrlsFunction())
				.setRobotsFunction(new CheckUrlWithRobotsFunction());
			
			builder.build().execute();
		} catch (Throwable t) {
			System.err.println("Error running CrawlTool: " + t.getMessage());
			t.printStackTrace(System.err);
			System.exit(-1);
		}
	}

}
