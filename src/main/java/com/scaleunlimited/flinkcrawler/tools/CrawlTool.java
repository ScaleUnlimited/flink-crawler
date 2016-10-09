package com.scaleunlimited.flinkcrawler.tools;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.scaleunlimited.flinkcrawler.functions.LengthenUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.NormalizeUrlsFunction;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;

public class CrawlTool {

	public static void main(String[] args) {
		
		// Generate topology, run it
		// TODO create real fetcher, real CrawlDB, inject into new CrawlTopology
		//   hmm, probably want to create a CrawlTopologyBuilder, since there's likely a lot
		// of settings we'd want to inject.
		// TODO execute the CrawlTopology
		
		try {
			StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
			
			DataStream<RawUrl> rawUrls = see.addSource(new SeedUrlSource(1.0f, "http://cnn.com", "http://facebook.com")).setParallelism(4);
			
			IterativeStream<RawUrl> iteration = rawUrls.iterate();
			DataStream<RawUrl> cleanedUrls = iteration.flatMap(new LengthenUrlsFunction())
													.flatMap(new NormalizeUrlsFunction());
			iteration.closeWith(cleanedUrls);

			cleanedUrls.print();

			see.execute();
		} catch (Throwable t) {
			System.err.println("Error running CrawlTool: " + t.getMessage());
			t.printStackTrace(System.err);
			System.exit(-1);
		}
	}

}
