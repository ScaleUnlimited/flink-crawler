package com.scaleunlimited.flinkcrawler.tools;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.scaleunlimited.flinkcrawler.functions.ValidUrlsFilter;
import com.scaleunlimited.flinkcrawler.functions.LengthenUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.NormalizeUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.OutlinkToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseFunction;
import com.scaleunlimited.flinkcrawler.functions.RawToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.SeedUrlSource;
import com.scaleunlimited.flinkcrawler.sources.TickleSource;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlValidator;

/**
 * A Flink streaming workflow that can be executed.
 * 
 * State Checkpoints in Iterative Jobs

Flink currently only provides processing guarantees for jobs without iterations.
Enabling checkpointing on an iterative job causes an exception. 
In order to force checkpointing on an iterative program the user needs to set 
a special flag when enabling checkpointing: env.enableCheckpointing(interval, force = true).

Please note that records in flight in the loop edges (and the state changes associated with them) 
will be lost during failure.
 *
 */
public class CrawlTopology {

	private StreamExecutionEnvironment _env;
	private String _jobName;
	
	protected CrawlTopology(StreamExecutionEnvironment env, String jobName) {
		_env = env;
		_jobName = jobName;
	}
	
	
	public JobExecutionResult execute() throws Exception {
		return _env.execute(_jobName);
	}
	
	public static class CrawlTopologyBuilder {
		
		private StreamExecutionEnvironment _env;
		private String _jobName = "flink-crawler";
		private long _tickleInterval = TickleSource.DEFAULT_TICKLE_INTERVAL;
		private long _runTime = TickleSource.INFINITE_RUN_TIME;
		private RichCoFlatMapFunction<CrawlStateUrl, Tuple0, FetchUrl> _crawlDBFunction;
		private RichCoFlatMapFunction<FetchUrl, Tuple0, FetchUrl> _robotsFunction;
		private RichCoFlatMapFunction<FetchUrl, Tuple0, FetchedUrl> _fetchFunction;
		private RichFlatMapFunction<FetchedUrl, Tuple2<ExtractedUrl, ParsedUrl>> _parseFunction;
		private SinkFunction<ParsedUrl> _contentSink;
		private BaseUrlNormalizer _urlNormalizer;
		private BaseUrlValidator _urlFilter;
		
		public CrawlTopologyBuilder(StreamExecutionEnvironment env) {
			_env = env;
		}

		public CrawlTopologyBuilder setJobName(String jobName) {
			_jobName = jobName;
			return this;
		}
		
		public CrawlTopologyBuilder setCrawlDBFunction(RichCoFlatMapFunction<CrawlStateUrl, Tuple0, FetchUrl> crawlDBFunction) {
			_crawlDBFunction = crawlDBFunction;
			return this;
		}
		
		public CrawlTopologyBuilder setRobotsFunction(RichCoFlatMapFunction<FetchUrl, Tuple0, FetchUrl> robotsFunction) {
			_robotsFunction = robotsFunction;
			return this;
		}
		
		public CrawlTopologyBuilder setFetchFunction(RichCoFlatMapFunction<FetchUrl, Tuple0, FetchedUrl> fetchFunction) {
			_fetchFunction = fetchFunction;
			return this;
		}
		
		public CrawlTopologyBuilder setParseFunction(RichFlatMapFunction<FetchedUrl, Tuple2<ExtractedUrl, ParsedUrl>> parseFunction) {
			_parseFunction = parseFunction;
			return this;
		}
		
		public CrawlTopologyBuilder setContentSink(SinkFunction<ParsedUrl> contentSink) {
			_contentSink = contentSink;
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
		
		public CrawlTopologyBuilder setTickleInterval(int tickleInterval) {
			_tickleInterval = tickleInterval;
			return this;
		}
		
		public CrawlTopologyBuilder setRunTime(long runTime) {
			_runTime = runTime;
			return this;
		}
		
		@SuppressWarnings("serial")
		public CrawlTopology build() {
			// TODO set source as a separate call. And use a simple collection source for testing.
			DataStream<RawUrl> rawUrls = _env.addSource(new SeedUrlSource(1.0f, "http://cnn.com", "http://facebook.com")).setParallelism(4);

			DataStream<Tuple0> tickler = _env.addSource(new TickleSource(_runTime, _tickleInterval));

			// TODO use something like double the fetch timeout here? or add fetch timeout to parse timeout? Maybe we
			// need to be able to ask each operation how long it might take, and use that. Note that as long as the
			// TickleSource keeps pumping out tickle tuples we won't terminate, so we don't need to worry about
			// something like a full CrawlDB merge causing us to time out. So in that case maybe just use double the
			// tickle interval here? But setting it to 200 when tickle is 100 causes us to not terminate :(
			IterativeStream<RawUrl> iteration = rawUrls.iterate(1000);
			DataStream<CrawlStateUrl> cleanedUrls = iteration.connect(tickler)
					.flatMap(new LengthenUrlsFunction())
					.flatMap(new NormalizeUrlsFunction(_urlNormalizer))
					.filter(new ValidUrlsFilter(_urlFilter))
					.map(new RawToStateUrlFunction());

			DataStream<FetchUrl> urlsToFetch = cleanedUrls.connect(tickler)
					.flatMap(_crawlDBFunction)
					.connect(tickler)
					.flatMap(_robotsFunction);
			// TODO need to split this stream and send rejected URLs back to crawlDB. Probably need to
			// merge this CrawlStateUrl stream with CrawlStateUrl streams from outlinks and fetch results.

			// TODO need a Tuple3 with a FetchedUrl(?) that has status update, which we can merge back in with
			// rejected robots URLs and outlinks. The fetcher code would want to handle settings no outlink(s) or
			// content and just a FetchedUrl for case of a fetch failure or if the content fetched isn't the
			// type that we want (e.g. image file)
			DataStream<Tuple2<ExtractedUrl, ParsedUrl>> fetchedUrls = urlsToFetch.connect(tickler)
					.flatMap(_fetchFunction)
					.flatMap(_parseFunction);

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

			// Save off parsed page content. So just extract the parsed content piece of the Tuple2, and
			// then pass it on to the provided content sink function.
			outlinksOrContent.select("content")
					.map(new MapFunction<Tuple2<ExtractedUrl,ParsedUrl>, ParsedUrl>() {

						@Override
						public ParsedUrl map(Tuple2<ExtractedUrl, ParsedUrl> in) throws Exception {
							return in.f1;
						}
					})
					.addSink(_contentSink);
			
			return new CrawlTopology(_env, _jobName);
		}
	}
}
