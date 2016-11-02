package com.scaleunlimited.flinkcrawler.tools;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDB;
import com.scaleunlimited.flinkcrawler.fetcher.BaseFetcher;
import com.scaleunlimited.flinkcrawler.functions.CrawlDBFunction;
import com.scaleunlimited.flinkcrawler.functions.FetchUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.LengthenUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.NormalizeUrlsFunction;
import com.scaleunlimited.flinkcrawler.functions.OutlinkToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.functions.ParseFunction;
import com.scaleunlimited.flinkcrawler.functions.RawToStateUrlFunction;
import com.scaleunlimited.flinkcrawler.functions.ValidUrlsFilter;
import com.scaleunlimited.flinkcrawler.parser.BaseParser;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.sources.BaseUrlSource;
import com.scaleunlimited.flinkcrawler.sources.TickleSource;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlLengthener;
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
		
		private BaseUrlSource _urlSource;
		
		private BaseCrawlDB _crawlDB;
		private RichCoFlatMapFunction<FetchUrl, Tuple0, FetchUrl> _robotsFunction;
		
		private BaseUrlLengthener _urlLengthener;
		private SinkFunction<ParsedUrl> _contentSink;
		private BaseUrlNormalizer _urlNormalizer;
		private BaseUrlValidator _urlFilter;
		private BaseFetcher _fetcher;
		private BaseParser _parser;
		
		public CrawlTopologyBuilder(StreamExecutionEnvironment env) {
			_env = env;
		}

		public CrawlTopologyBuilder setJobName(String jobName) {
			_jobName = jobName;
			return this;
		}
		
		public CrawlTopologyBuilder setUrlSource(BaseUrlSource urlSource) {
			_urlSource = urlSource;
			return this;
		}
		
		public CrawlTopologyBuilder setUrlLengthener(BaseUrlLengthener lengthener) {
			_urlLengthener = lengthener;
			return this;
		}
		
		public CrawlTopologyBuilder setCrawlDB(BaseCrawlDB crawlDB) {
			_crawlDB = crawlDB;
			return this;
		}
		
		public CrawlTopologyBuilder setRobotsFunction(RichCoFlatMapFunction<FetchUrl, Tuple0, FetchUrl> robotsFunction) {
			_robotsFunction = robotsFunction;
			return this;
		}
		
		public CrawlTopologyBuilder setFetcher(BaseFetcher fetcher) {
			_fetcher = fetcher;
			return this;
		}
		
		public CrawlTopologyBuilder setParser(BaseParser parser) {
			_parser = parser;
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
			// TODO use single topology parallelism? But likely will want different levels for differnt parts.
			DataStream<RawUrl> rawUrls = _env.addSource(_urlSource).setParallelism(4);

			DataStream<Tuple0> tickler = _env.addSource(new TickleSource(_runTime, _tickleInterval));

			// TODO use something like double the fetch timeout here? or add fetch timeout to parse timeout? Maybe we
			// need to be able to ask each operation how long it might take, and use that. Note that as long as the
			// TickleSource keeps pumping out tickle tuples we won't terminate, so we don't need to worry about
			// something like a full CrawlDB merge causing us to time out. So in that case maybe just use double the
			// tickle interval here? But setting it to 200 when tickle is 100 causes us to not terminate :(
			IterativeStream<RawUrl> iteration = rawUrls.iterate(1000);
			DataStream<CrawlStateUrl> cleanedUrls = iteration.connect(tickler)
					.flatMap(new LengthenUrlsFunction(_urlLengthener))
					.name("LengthenUrlsFunction")
					.flatMap(new NormalizeUrlsFunction(_urlNormalizer))
					.name("NormalizeUrlsFunction")
					.filter(new ValidUrlsFilter(_urlFilter))
					.name("FilterUrlsFunction")
					.map(new RawToStateUrlFunction());

			DataStream<FetchUrl> urlsToFetch = cleanedUrls.connect(tickler)
					.flatMap(new CrawlDBFunction(_crawlDB))
					.connect(tickler)
					.flatMap(_robotsFunction);
			// TODO need to split this stream and send rejected URLs back to crawlDB. Probably need to
			// merge this CrawlStateUrl stream with CrawlStateUrl streams from outlinks and fetch results.

			// TODO need a Tuple3 with a FetchedUrl(?) that has status update, which we can merge back in with
			// rejected robots URLs and outlinks. The fetcher code would want to handle settings no outlink(s) or
			// content and just a FetchedUrl for case of a fetch failure or if the content fetched isn't the
			// type that we want (e.g. image file)
			DataStream<Tuple2<ExtractedUrl, ParsedUrl>> fetchedUrls = urlsToFetch.connect(tickler)
					.flatMap(new FetchUrlsFunction(_fetcher))
					.flatMap(new ParseFunction(_parser));

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
