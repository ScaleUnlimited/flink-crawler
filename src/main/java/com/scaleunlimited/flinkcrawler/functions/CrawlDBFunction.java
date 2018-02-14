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
 * The Flink operator that managed the "crawl DB". Incoming URLs are merged in memory, using MapState
 * with key == URL hash, value = CrawlStateUrl. Whenever we get a "tickler" URL, we emit URLs from the fetch queue
 * (if available). Whenever we get a "domain" URL, we add URLs for that domain to the fetch queue.
 * 
 * The use of the fetch queue lets us apply some heuristics to fetching the "best" (approximately)
 * URLs, without having to scan every URL.
 */
@SuppressWarnings("serial")
public class CrawlDBFunction extends BaseFlatMapFunction<CrawlStateUrl, FetchUrl> {
    static final Logger LOGGER = LoggerFactory.getLogger(CrawlDBFunction.class);

    private static final int URLS_PER_TICKLE = 10;
    private static final int MAX_IN_FLIGHT_URLS = 5; // TODO(kkrugler) - revert to URLS_PER_TICKLE * 3;

    private BaseCrawlDBMerger _merger;

    // List of URLs that are available to be fetched.
    private final FetchQueue _fetchQueue;

    private transient AtomicInteger _numInFlightUrls;

    private transient MapState<Long, CrawlStateUrl> _activeUrls;
    private transient MapState<Integer, Long> _activeUrlsIndex;
    private transient ValueState<Integer> _numActiveUrls;
    private transient MapState<Long, CrawlStateUrl> _archivedUrls;

    private transient Random _rand;

    private transient CrawlStateUrl _mergedUrlState;

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

        _mergedUrlState = new CrawlStateUrl();
        
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
        MapStateDescriptor<Long, CrawlStateUrl> archivedUrlsStateDescriptor = new MapStateDescriptor<>("archived-urls",
                Long.class, CrawlStateUrl.class);
        _archivedUrls = getRuntimeContext().getMapState(archivedUrlsStateDescriptor);

        // 4. Index to hash mapping for active URLs (key = index, value = url hash)
        //
        // So if we have an index from 0...<active url count>-1, we can look up the
        // hash, and then use that to find the actual CrawlStateUrl in the active
        // URLs MapState.
        MapStateDescriptor<Integer, Long> activeUrlsIndexStateDescriptor = new MapStateDescriptor<>("active-urls-index",
                Integer.class, Long.class);
        _activeUrlsIndex = getRuntimeContext().getMapState(activeUrlsIndexStateDescriptor);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void flatMap(CrawlStateUrl url, Collector<FetchUrl> collector) throws Exception {
        if (url.getUrlType() == UrlType.REGULAR) {
            processRegularUrl(url, collector);
        } else if (url.getUrlType() == UrlType.TICKLER) {
            processTicklerUrl(collector);
        } else if (url.getUrlType() == UrlType.DOMAIN) {
            processDomainUrl(url.getPld(), collector);
        } else if (url.getUrlType() == UrlType.TERMINATION) {
            processTerminationUrl();
        } else {
            throw new RuntimeException("Unknown URL type: " + url.getUrlType());
        }
    }

    /**
     * We got a "termination" URL, which tells us to drain our fetch queue, and not
     * add any more URLs to this queue (ignore domain URLs).
     */
    private void processTerminationUrl() {
        LOGGER.info(String.format("CrawlDBFunction (%d/%d) terminating", _partition, _parallelism));
        // TODO flush queue, set flag
    }

    /**
     * We got a "domain" URL, which triggers adding URLs for that domain to the fetch queue.
     * 
     * @param pld
     * @param collector
     * @throws Exception
     */
    private void processDomainUrl(String pld, Collector<FetchUrl> collector) throws Exception {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Got domain URL for " + pld);
        }
        
        Integer numUrls = _numActiveUrls.value();
        if (numUrls == null) {
            LOGGER.warn("Houston, we have a problem - null active URL count for domain " + pld);
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
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(String.format(
                            "CrawlDBFunction (%d/%d) got result %s adding '%s' to fetch queue",
                            _partition, _parallelism,
                            queueResult, stateUrl));
                }
                
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
    }

    /**
     * We received a tickler URL that triggers emitting of URLs from the fetch queue
     * to the collector.
     * 
     * @param collector
     */
    private void processTicklerUrl(Collector<FetchUrl> collector) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("CrawlDBFunction (%d/%d) checking for URLs to emit", _partition, _parallelism));
        }

        for (int i = 0; i < URLS_PER_TICKLE; i++) {
            int activeUrls = _numInFlightUrls.get();
            if (activeUrls > MAX_IN_FLIGHT_URLS) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(String.format("CrawlDBFunction (%d/%d) skipping emit, too many active URLs (%d)",
                            _partition, _parallelism, activeUrls));
                }
                
                return;
            }

            FetchUrl fetchUrl = _fetchQueue.poll();

            if (fetchUrl != null) {
                collector.collect(fetchUrl);
                int nowActive = _numInFlightUrls.incrementAndGet();
                LOGGER.debug(String.format("CrawlDBFunction (%d/%d) emitted URL %s (%d active)", _partition,
                        _parallelism, fetchUrl, nowActive));
            } else {
                break;
            }
        }
    }

    /**
     * We received a regular URL that we need to add (or merge) into our crawl state.
     * 
     * @param url
     * @param collector
     * @throws Exception
     */
    private void processRegularUrl(CrawlStateUrl url, Collector<FetchUrl> collector) throws Exception {
        record(this.getClass(), url, FetchStatus.class.getSimpleName(), url.getStatus().toString());

        // If it's not an unfetched URL, we can decrement our active URLs
        FetchStatus newStatus = url.getStatus();
        if (newStatus != FetchStatus.UNFETCHED) {
            if (_numInFlightUrls.decrementAndGet() < 0) {
                LOGGER.warn(String.format("CrawlDBFunction (%d/%d) has negative in-flight URLs", _partition, _parallelism));
            }
        }

        Long urlHash = url.makeKey();
        if (_archivedUrls.contains(urlHash)) {
            // It's been archived, ignore...
            
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format(
                        "CrawlDBFunction (%d/%d) ignoring archived URL %s with hash %d",
                        _partition, _parallelism, url, urlHash));
            }
            
            // If the state is unfetched, we're all good, but if not then that's a logical error
            // as we shouldn't be emitting URLs that are archived.
            if (newStatus != FetchStatus.UNFETCHED) {
                LOGGER.error(String.format(
                        "CrawlDBFunction (%d/%d) got archived URL %s with active status %s",
                        _partition, _parallelism, url, newStatus));
            }
        } else {
            CrawlStateUrl stateUrl = _activeUrls.get(urlHash);
            if (stateUrl == null) {
                
                // We've never seen this URL before.
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format(
                            "CrawlDBFunction (%d/%d) adding new URL %s with hash %d",
                            _partition, _parallelism, url, urlHash));
                }

                // Better be unfetched.
                if (newStatus != FetchStatus.UNFETCHED) {
                    LOGGER.error(String.format(
                            "CrawlDBFunction (%d/%d) got new URL '%s' with active status %s",
                            _partition, _parallelism, url, newStatus));
                    return;
                }
                
                CounterUtils.increment(getRuntimeContext(), FetchStatus.UNFETCHED);

                // TODO need to copy URL if object reuse enabled?
                _activeUrls.put(urlHash, url);
                
                Integer numActiveUrls = _numActiveUrls.value();
                if (numActiveUrls == null) {
                    numActiveUrls = 0;
                }

                _activeUrlsIndex.put(numActiveUrls, urlHash);
                _numActiveUrls.update(numActiveUrls + 1);
            } else {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(String.format(
                            "CrawlDBFunction (%d/%d) needs to merge incoming URL %s with %s (hash %d)",
                            _partition, _parallelism, url, stateUrl, urlHash));
                }

                FetchStatus oldStatus = stateUrl.getStatus();
                if (mergeUrls(stateUrl, url)) {
                    _activeUrls.put(urlHash, stateUrl);
                    
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format(
                                "CrawlDBFunction (%d/%d) updated state of URL %s (hash %d)",
                                _partition, _parallelism, stateUrl, urlHash));
                    }
                    
                    CounterUtils.decrement(getRuntimeContext(), oldStatus);
                    CounterUtils.increment(getRuntimeContext(), newStatus);
                }
            }
        }
    }
    
    private boolean mergeUrls(CrawlStateUrl stateUrl, CrawlStateUrl newUrl) {
        MergeResult result = _merger.doMerge(stateUrl, newUrl, _mergedUrlState);

        switch (result) {
            case USE_FIRST:
                // All set, stateUrl is what we want to use, so no update
                return false;

            case USE_SECOND:
                stateUrl.setFrom(newUrl);
                return true;

            case USE_MERGED:
                stateUrl.setFrom(_mergedUrlState);
                return true;

            default:
                throw new RuntimeException("Unknown merge result: " + result);
        }
    }

}
