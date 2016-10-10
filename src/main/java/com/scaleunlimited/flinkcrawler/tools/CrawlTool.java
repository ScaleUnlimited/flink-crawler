package com.scaleunlimited.flinkcrawler.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.scaleunlimited.flinkcrawler.functions.CheckUrlWithRobotsFunction;
import com.scaleunlimited.flinkcrawler.functions.CrawlDBFunction;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.FilterUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.LengthenUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.RawToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.functions.NormalizeUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.OutlinkToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseFunction;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.TickleSource;
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
			DataStream<Tuple0> tickler = see.addSource(new TickleSource());
			
			IterativeStream<RawUrl> iteration = rawUrls.iterate();
			DataStream<CrawlStateUrl> cleanedUrls = iteration.connect(tickler)
					.flatMap(new LengthenUrlsFunction())
					.flatMap(new NormalizeUrlsFunction())
					.flatMap(new FilterUrlsFunction())
					.map(new RawToStateUrlFunction());
			
			DataStream<FetchUrl> urlsToFetch = cleanedUrls.connect(tickler)
					.flatMap(new CrawlDBFunction())
					.connect(tickler)
					.flatMap(new CheckUrlWithRobotsFunction());
			// TODO need to split this stream and send rejected URLs back to crawlDB. Probably need to
			// merge this CrawlStateUrl stream with CrawlStateUrl streams from outlinks and fetch results.
				
			
			DataStream<Tuple2<ExtractedUrl, ParsedUrl>> fetchedUrls = urlsToFetch.connect(tickler)
					.flatMap(new FetchUrlsFunction())
					.flatMap(new ParseFunction());
			
			// Need to split this stream and send extracted URLs back, and save off parsed page content.
			SplitStream<Tuple2<ExtractedUrl,ParsedUrl>> outlinksOrContent = fetchedUrls.split(new OutputSelector<Tuple2<ExtractedUrl,ParsedUrl>>() {
				
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
					.map(new OutlinkToStateUrlFunction());
			
			iteration.closeWith(newUrls);

			cleanedUrls.print();

			see.execute();
		} catch (Throwable t) {
			System.err.println("Error running CrawlTool: " + t.getMessage());
			t.printStackTrace(System.err);
			System.exit(-1);
		}
	}

}
