package com.scaleunlimited.flinkcrawler.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;

public class MyIterator {
    public static void main(String[] args) throws Exception {
    	StreamExecutionEnvironment see = 
			StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<RawUrl> urls = 
			see.addSource(new SeedUrlSource(1.0f, "http://cnn.com", "http://facebook.com"));
    		
    	IterativeStream<RawUrl> iteration = urls.iterate();
    	DataStream<RawUrl> iterationBody =
			iteration.flatMap(new FlatMapFunction<RawUrl, RawUrl>() {

				@Override
				public void flatMap(RawUrl inputUrl, Collector<RawUrl> collector)
					throws Exception {
					
					String urlString = inputUrl.getUrl();
					String addedUrlString = getNextUrlString(urlString);
					collector.collect(inputUrl);
					float addedUrlScore = inputUrl.getEstimatedScore() * 0.5f;
					RawUrl addedUrl =
						new RawUrl(addedUrlString, addedUrlScore);
					collector.collect(addedUrl);
				}
			});
	}
    
	public static String getNextUrlString(String urlString) {
		int dashPos = urlString.lastIndexOf("-");
		int urlIndex = 
			Integer.parseInt(urlString.substring(dashPos + 1));
		return urlString.substring(0, dashPos) + "-" + (urlIndex + 1);
	}
}
