package com.scaleunlimited.flinkcrawler.functions;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger;
import com.scaleunlimited.flinkcrawler.crawldb.BaseCrawlDBMerger.MergeResult;
import com.scaleunlimited.flinkcrawler.metrics.CounterUtils;
import com.scaleunlimited.flinkcrawler.metrics.CrawlerMetrics;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.pojos.UrlType;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue.FetchQueueResult;

/**
 * The Flink operator that managed the "crawl DB". Incoming URLs are de-duplicated and merged in memory, using MapState
 * with key == URL hash, value = CrawlStateUrl. Whenever we get a "tickler" URL, we will emit URLs from the fetch queue
 * (if available). Whenever we get a "domain" URL, we will add URLs for that domain to the fetch queue.
 */
@SuppressWarnings("serial")
public class CrawlDBFunction extends BaseFlatMapFunction<CrawlStateUrl, FetchUrl> {
    static final Logger LOGGER = LoggerFactory.getLogger(CrawlDBFunction.class);

    private static final int URLS_PER_TICKLE = 10;
    private static final int MAX_IN_FLIGHT_URLS = URLS_PER_TICKLE * 3;

    private BaseCrawlDBMerger _merger;

    // List of URLs that are available to be fetched.
    private final FetchQueue _fetchQueue;

    private transient AtomicInteger _numInFlightUrls;

    private transient MapState<Long, CrawlStateUrl> _activeUrls;
    private transient MapState<Integer, Long> _activeUrlsIndex;
    private transient ValueState<Integer> _numActiveUrls;
    private transient MapState<Long, CrawlStateUrl> _archivedUrls;

    private transient Random _rand;

    private transient byte[] _firstUrlState;
    private transient byte[] _secondUrlState;
    private transient byte[] _mergedUrlState;

    public CrawlDBFunction(BaseCrawlDBMerger merger, FetchQueue fetchQueue) {
        _merger = merger;
        _fetchQueue = fetchQueue;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        RuntimeContext context = getRuntimeContext();

        context.getMetricGroup().gauge(CrawlerMetrics.GAUGE_URLS_IN_FETCH_QUEUE.toString(), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return _fetchQueue.size();
            }
        });

        // Track how many URLs we think are being processed.
        context.getMetricGroup().gauge(CrawlerMetrics.GAUGE_URLS_IN_FLIGHT.toString(), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return _numInFlightUrls.get();
            }
        });

        _numInFlightUrls = new AtomicInteger(0);

        _fetchQueue.open();

        _rand = new Random(0L);

        // Create three keyed/managed states

        // 1. Active URLs: MapState (key = url hash, value = CrawlStateUrl)
        MapStateDescriptor<Long, CrawlStateUrl> urlStateDescriptor = new MapStateDescriptor<>("active-urls", Long.class,
                CrawlStateUrl.class);
        _activeUrls = getRuntimeContext().getMapState(urlStateDescriptor);

        // 2. Active URL count: ValueState (value = number of entries in MapState)
        ValueStateDescriptor<Integer> urlCountDescriptor = new ValueStateDescriptor<>("num-active-urls",
                TypeInformation.of(new TypeHint<Integer>() {
                }));
        _numActiveUrls = getRuntimeContext().getState(urlCountDescriptor);

        // 3. Archived URLs: MapState (key = url hash, value = CrawlStateUrl)
        MapStateDescriptor<Long, CrawlStateUrl> archivedUrlsStateDescriptor = new MapStateDescriptor<>("archived-urls", // the
                                                                                                                        // state
                                                                                                                        // name
                Long.class, CrawlStateUrl.class);
        _archivedUrls = getRuntimeContext().getMapState(archivedUrlsStateDescriptor);

        // 4. Index to hash mapping for active URLs (key = index, value = url hash)
        MapStateDescriptor<Integer, Long> activeUrlsIndexStateDescriptor = new MapStateDescriptor<>("active-urls-index", // the
                                                                                                                         // state
                                                                                                                         // name
                Integer.class, Long.class);
        _activeUrlsIndex = getRuntimeContext().getMapState(activeUrlsIndexStateDescriptor);

        _firstUrlState = new byte[CrawlStateUrl.VALUE_SIZE];
        _secondUrlState = new byte[CrawlStateUrl.VALUE_SIZE];
        _mergedUrlState = new byte[CrawlStateUrl.VALUE_SIZE];
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void flatMap(CrawlStateUrl url, Collector<FetchUrl> collector) throws Exception {

        if (url.getUrlType() == UrlType.REGULAR) {
            FetchStatus status = url.getStatus();
            CounterUtils.increment(getRuntimeContext(), status);

            // Decrement the UNFETCHED counter (when we have a status that isn't UNFETCHED)
            if (status != FetchStatus.UNFETCHED) {
                CounterUtils.increment(getRuntimeContext(), FetchStatus.UNFETCHED, -1);
            }

            record(this.getClass(), url, FetchStatus.class.getSimpleName(), url.getStatus().toString());

            Long urlHash = url.makeKey();
            if (_archivedUrls.contains(urlHash)) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Ignoring archived URL: " + url);
                }
                // It's been archived, ignore
            } else {
                CrawlStateUrl stateUrl = _activeUrls.get(urlHash);
                if (stateUrl == null) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Adding new URL to crawlDB: " + url);
                    }

                    // TODO need to copy URL if object reuse enabled?
                    _activeUrls.put(urlHash, url);
                    Integer numActiveUrls = _numActiveUrls.value();
                    if (numActiveUrls == null) {
                        numActiveUrls = 0;
                    }

                    _activeUrlsIndex.put(numActiveUrls, urlHash);
                    _numActiveUrls.update(numActiveUrls + 1);
                } else if (mergeUrls(url, stateUrl)) {
                    _activeUrls.put(urlHash, stateUrl);
                }
            }

            // If it's not an unfetched URL, we can decrement our active URLs
            if (status != FetchStatus.UNFETCHED) {
                if (_numInFlightUrls.decrementAndGet() < 0) {
                    LOGGER.warn("Houston, we have a problem - negative in-flight URLs");
                }
            }
        } else if (url.getUrlType() == UrlType.TICKLER) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Got tickler URL");
            }
            
            LOGGER.trace(String.format("CrawlDBFunction (%d/%d) checking for URLs to emit", _partition, _parallelism));

            for (int i = 0; i < URLS_PER_TICKLE; i++) {
                int activeUrls = _numInFlightUrls.get();
                if (activeUrls > MAX_IN_FLIGHT_URLS) {
                    LOGGER.trace(String.format("CrawlDBFunction (%d/%d) skipping emit, too many active URLs (%d)",
                            _partition, _parallelism, activeUrls));
                    return;
                }

                FetchUrl fetchUrl = _fetchQueue.poll();

                if (fetchUrl != null) {
                    LOGGER.debug(String.format("CrawlDBFunction (%d/%d) emitting URL %s", _partition, _parallelism,
                            fetchUrl));
                    collector.collect(fetchUrl);
                    int nowActive = _numInFlightUrls.incrementAndGet();
                    LOGGER.debug(String.format("CrawlDBFunction (%d/%d) emitted URL %s (%d active)", _partition,
                            _parallelism, fetchUrl, nowActive));
                } else {
                    break;
                }
            }
        } else if (url.getUrlType() == UrlType.DOMAIN) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Got domain URL for " + url.getPld());
            }
            
            Integer numUrls = _numActiveUrls.value();
            if (numUrls == null) {
                LOGGER.warn("Houston, we have a problem - null active URL count for domain " + url.getPld());
            } else {
                // We want to find URLs that are candidates for fetching, and add to the queue. But we
                // want to pick a random set of these, so start at a random offset and loop around,
                // and only add some of them.
                int urlsToAdd = Math.max(10, Math.min(100, numUrls / 5));
                int urlsAdded = 0;
                int startingIndex = _rand.nextInt(numUrls);
                for (int i = 0; (i < numUrls) && (urlsAdded < urlsToAdd); i++) {
                    int index = (startingIndex + i) % numUrls;
                    CrawlStateUrl stateUrl = _activeUrls.get(_activeUrlsIndex.get(index));
                    FetchQueueResult queueResult = _fetchQueue.add(stateUrl);
                    switch (queueResult) {
                        case QUEUED:
                            // TODO so I don't have to do MapState.put(hash, stateUrl) to get this
                            // updated, right?
                            stateUrl.setStatus(FetchStatus.FETCHING);
                            urlsAdded += 1;
                            break;

                        case ACTIVE:
                            break;

                        case ARCHIVE:
                            // TODO remove from active url state, add to archive state, update
                            // URL count and remove mapping from index to hash.
                            break;

                        default:
                            throw new RuntimeException("Unknown fetch queue result: " + queueResult);
                    }
                }
            }
        } else if (url.getUrlType() == UrlType.TERMINATION) {
            LOGGER.info(String.format("CrawlDBFunction (%d/%d) terminating", _partition, _parallelism));
            // TODO flush queue, set flag
        } else {
            throw new RuntimeException("Unknown URL type: " + url.getUrlType());
        }
    }

    private boolean mergeUrls(CrawlStateUrl newUrl, CrawlStateUrl stateUrl) {
        newUrl.getValue(_firstUrlState);
        stateUrl.getValue(_secondUrlState);
        MergeResult result = _merger.doMerge(_firstUrlState, _secondUrlState, _mergedUrlState);

        switch (result) {
            case USE_FIRST:
                stateUrl.setFromValue(_firstUrlState);
                return true;

            case USE_SECOND:
                return false;

            case USE_MERGED:
                stateUrl.setFromValue(_mergedUrlState);
                return true;

            default:
                throw new RuntimeException("Unknown merge result: " + result);
        }
    }

}
