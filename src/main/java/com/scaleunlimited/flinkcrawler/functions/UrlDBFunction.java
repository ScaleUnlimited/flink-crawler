package com.scaleunlimited.flinkcrawler.functions;

import java.util.HashMap;
import java.util.Map;
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
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.config.CrawlTerminator;
import com.scaleunlimited.flinkcrawler.metrics.CounterUtils;
import com.scaleunlimited.flinkcrawler.metrics.CrawlerMetrics;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.urldb.BaseUrlStateMerger;
import com.scaleunlimited.flinkcrawler.urldb.BaseUrlStateMerger.MergeResult;
import com.scaleunlimited.flinkcrawler.utils.FetchQueue;

/**
 * The Flink operator that managed the URL portion of the "crawl DB". Incoming URLs are merged in memory, 
 * using a map with key == URL hash, value = CrawlStateUrl. Whenever our timer fires, we emit 
 * URLs from the fetch queue (if available).
 * 
 * The use of the fetch queue lets us apply some heuristics to fetching the "best" (approximately) URLs, without having
 * to scan every URL.
 */
@SuppressWarnings("serial")
public class UrlDBFunction extends BaseKeyedProcessFunction<String, CrawlStateUrl, FetchUrl> implements CheckpointedFunction {
    static final Logger LOGGER = LoggerFactory.getLogger(UrlDBFunction.class);

    // When we have to update the status of a URL in the fetch queue (because we're either fetching it, or it's
    // getting booted by a better URL) we output it via this side channel.
    public static final OutputTag<CrawlStateUrl> STATUS_OUTPUT_TAG 
        = new OutputTag<CrawlStateUrl>("status"){};

    private static final int MAX_IN_FLIGHT_URLS = 100;

    private BaseUrlStateMerger _merger;
    private CrawlTerminator _terminator;

    // List of URLs that are available to be fetched.
    private final FetchQueue _fetchQueue;

    private transient AtomicInteger _numInFlightUrls;

    private transient MapState<Long, CrawlStateUrl> _activeUrls;
    private transient MapState<Integer, Long> _activeUrlsIndex;
    private transient ValueState<Integer> _numActiveUrls;
    private transient ValueState<Integer> _activeIndex;
    private transient MapState<Long, CrawlStateUrl> _archivedUrls;

    private transient CrawlStateUrl _mergedUrlState;

    // TODO(kkrugler) remove this debugging code.
    private transient Map<String, Long> _inFlightUrls;

    public UrlDBFunction(CrawlTerminator terminator, BaseUrlStateMerger merger, FetchQueue fetchQueue) {
        _terminator = terminator;
        _merger = merger;
        _fetchQueue = fetchQueue;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Create our keyed/managed states

        // 1. Active URLs: MapState (key = url hash, value = CrawlStateUrl)
        MapStateDescriptor<Long, CrawlStateUrl> urlStateDescriptor = new MapStateDescriptor<>(
                "active-urls", Long.class, CrawlStateUrl.class);
        _activeUrls = getRuntimeContext().getMapState(urlStateDescriptor);

        // 2. Active URL count: ValueState (value = number of entries in Active ULRs MapState)
        ValueStateDescriptor<Integer> urlCountDescriptor = new ValueStateDescriptor<>(
                "num-active-urls", TypeInformation.of(new TypeHint<Integer>() {
                }));
        _numActiveUrls = getRuntimeContext().getState(urlCountDescriptor);

        // 3. Index to next active URL: ValueState (Value = index of next URL to try to queue)
        ValueStateDescriptor<Integer> urlIndexDescriptor = new ValueStateDescriptor<>(
                "cur-url-index", TypeInformation.of(new TypeHint<Integer>() {
                }));
        _activeIndex = getRuntimeContext().getState(urlIndexDescriptor);
        
        // 4. Archived URLs: MapState (key = url hash, value = CrawlStateUrl)
        MapStateDescriptor<Long, CrawlStateUrl> archivedUrlsStateDescriptor = new MapStateDescriptor<>(
                "archived-urls", Long.class, CrawlStateUrl.class);
        _archivedUrls = getRuntimeContext().getMapState(archivedUrlsStateDescriptor);

        // 5. Index to hash mapping for active URLs (key = index, value = url hash)
        //
        // So if we have an index from 0...<active url count>-1, we can look up the
        // hash, and then use that to find the actual CrawlStateUrl in the active
        // URLs MapState.
        MapStateDescriptor<Integer, Long> activeUrlsIndexStateDescriptor = new MapStateDescriptor<>(
                "active-urls-index", Integer.class, Long.class);
        _activeUrlsIndex = getRuntimeContext().getMapState(activeUrlsIndexStateDescriptor);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        RuntimeContext context = getRuntimeContext();

        context.getMetricGroup().gauge(CrawlerMetrics.GAUGE_URLS_IN_FETCH_QUEUE.toString(),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return _fetchQueue.size();
                    }
                });

        // Track how many URLs we think are being processed.
        context.getMetricGroup().gauge(CrawlerMetrics.GAUGE_URLS_IN_FLIGHT.toString(),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return _numInFlightUrls.get();
                    }
                });

        _mergedUrlState = new CrawlStateUrl();

        _numInFlightUrls = new AtomicInteger(0);

        _fetchQueue.open();
        _terminator.open();

        _inFlightUrls = new HashMap<>();
    }

    @Override
    public void processElement(CrawlStateUrl url, Context ctx, Collector<FetchUrl> collector) throws Exception {
        record(this.getClass(), url, FetchStatus.class.getSimpleName(), url.getStatus().toString());

        // See if we have this domain already. If not, create a timer.
        long processingTime = ctx.timerService().currentProcessingTime();
        
        Integer numUrls = _numActiveUrls.value();
        if (numUrls == null) {
            // Create entries for this domain in the various states.
            _numActiveUrls.update(0);
            _activeIndex.update(0);
            
            // And we want to create a timer, so we have one per domain
            ctx.timerService().registerProcessingTimeTimer(processingTime + 100);
        }

        // Now update state for this URL, and potentially emit it if status is 'fetching'.
        processUrl(url, collector);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FetchUrl> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        
        // See if we've got a URL that we want to add to the fetch queue.
        if (!_terminator.isTerminated()) {
            addUrlToFetchQueue(ctx);
            emitUrlFromFetchQueue(ctx);
            
            ctx.timerService().registerProcessingTimeTimer(timestamp + 100);
        } else {
            LOGGER.info("Terminating timer for this domain");
        }
    }
    
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Nothing special we need to do here, since we have keyed state that gets
        // handled automatically.
    }

    @Override
    public void close() throws Exception {
        long curTime = System.currentTimeMillis();
        for (String url : _inFlightUrls.keySet()) {
            LOGGER.debug("{}\t{}", curTime - _inFlightUrls.get(url), url);
        }

        super.close();
    }

    /**
     * We got a "domain" URL, which triggers adding a URL for that domain to the fetch queue.
     * 
     * @param pld
     * @param collector
     * @throws Exception
     */
    private void addUrlToFetchQueue(Context context) throws Exception {
        final boolean doTracing = LOGGER.isTraceEnabled();

        Integer numUrls = _numActiveUrls.value();
        if (numUrls == null) {
            LOGGER.error("Houston, we have a problem - null active URL count for domain");
            return;
        }

        int index = _activeIndex.value();

        Long urlHash = _activeUrlsIndex.get(index);
        CrawlStateUrl stateUrl = _activeUrls.get(urlHash);
        CrawlStateUrl rejectedUrl = _fetchQueue.add(stateUrl);

        if (doTracing) {
            if (rejectedUrl == null) {
                LOGGER.trace(
                        "UrlDBFunction ({}/{}) added '{}' to fetch queue",
                        _partition, _parallelism, stateUrl);
            } else if (rejectedUrl == stateUrl) {
                // Happens constantly due to state, so just ignore
            } else {
                LOGGER.trace(
                        "UrlDBFunction ({}/{}) added '{}' to fetch queue, removing '{}'",
                        _partition, _parallelism, stateUrl, rejectedUrl);
            }
        }

        // If we get back null (there's space in the queue), or some other
        // URL (our URL was better) then this URL was added to the queue.
        if (rejectedUrl != stateUrl) {
            LOGGER.trace(
                    "UrlDBFunction ({}/{}) setting '{}' state status to QUEUED",
                    _partition, _parallelism, stateUrl);

            stateUrl.setStatus(FetchStatus.QUEUED);
            _activeUrls.put(urlHash, stateUrl);

            // If we just replaced a (lower scoring) URL on the fetch queue,
            // then we need to restore the rejected URL's status in the URL DB.
            if (rejectedUrl != null) {
                rejectedUrl.restorePreviousStatus();
                LOGGER.trace(
                        "UrlDBFunction ({}/{}) restored '{}' to previous status via side output",
                        _partition, _parallelism, rejectedUrl);

                context.output(STATUS_OUTPUT_TAG, rejectedUrl);

                // Otherwise we just added one URL to the queue (vs. replacing one already there)
            } else {
                CounterUtils.increment(getRuntimeContext(), FetchStatus.QUEUED);
            }
        }

        // Update index of the URL we should check the next time we get called.
        index = (index + 1) % numUrls;
        _activeIndex.update(index);
    }

    /**
     * See if there's a URL in the fetch queue that we should emit (via side output, so that
     * it loops around to the UrlDBFunction to update the status).
     * 
     * @param context
     */
    private void emitUrlFromFetchQueue(Context context) {
        final boolean doTracing = LOGGER.isTraceEnabled();

        int activeUrls = _numInFlightUrls.get();
        if (activeUrls > MAX_IN_FLIGHT_URLS) {
            if (doTracing) {
                LOGGER.trace(
                        "UrlDBFunction ({}/{}) skipping emit, too many active URLs ({})",
                        _partition, _parallelism, activeUrls);
            }

            return;
        }

        CrawlStateUrl crawlStateUrl = _fetchQueue.poll();
        if (crawlStateUrl != null) {

            // Update the state of the URL in the URL DB so we know it's no longer just queued,
            // but now about to be fetched.  It now goes into our side channel where it will
            // come back around to processRegularUrl which will save the state change 
            // and then begin fetching it.
            LOGGER.trace(
                    "UrlDBFunction ({}/{}) setting '{}' status to FETCHING via side output",
                    _partition, _parallelism, crawlStateUrl);

            crawlStateUrl.setStatus(FetchStatus.FETCHING);
            crawlStateUrl.setStatusTime(System.currentTimeMillis());
            context.output(STATUS_OUTPUT_TAG, crawlStateUrl);
        }
    }

    /**
     * We received a URL that we need to add (or merge) into our crawl state.
     * 
     * @param url
     * @param collector
     * @throws Exception
     */
    private void processUrl(CrawlStateUrl url, Collector<FetchUrl> collector)
            throws Exception {

        // If it's not an unfetched URL, we can decrement our active URLs
        FetchStatus newStatus = url.getStatus();
 
        if ((newStatus != FetchStatus.UNFETCHED) && (url.getStatusTime() == 0)) {
            throw new RuntimeException(
                    String.format("UrlDBFunction (%d/%d) got URL with invalid status time: %s",
                            _partition, _parallelism, url));
        }
        
        // If it's a URL just pulled from the fetch queue, then we need to emit it
        // for robots check.  Its status time should be newer than what's in the URL DB
        // so it's guaranteed to win below and update the URL DB as well.
        if (newStatus == FetchStatus.FETCHING) {
            FetchUrl fetchUrl = new FetchUrl(url, url.getScore());
            
            collector.collect(fetchUrl);
            int nowActive = _numInFlightUrls.incrementAndGet();
            LOGGER.trace("UrlDBFunction ({}/{}) emitted URL '{}' ({} active)",
                    _partition, _parallelism, fetchUrl, nowActive);
            
            _inFlightUrls.put(fetchUrl.getUrl(), System.currentTimeMillis());
        
        // Otherwise, if it's the result of the fetch attempt then it's no longer active.
        } else if (newStatus != FetchStatus.UNFETCHED) {
            Long startTime = _inFlightUrls.remove(url.getUrl());
            if (startTime == null) {
                throw new RuntimeException(
                        String.format("UrlDBFunction (%d/%d) got URL not in active state: %s",
                                _partition, _parallelism, url));
            }

            LOGGER.trace("{}ms to process '{}'", System.currentTimeMillis() - startTime, url);

            int nowActive = _numInFlightUrls.decrementAndGet();
            LOGGER.trace("UrlDBFunction ({}/{}) receiving URL {} ({} active)",
                    _partition, _parallelism, url, nowActive);

            if (nowActive < 0) {
                throw new RuntimeException(
                        String.format("UrlDBFunction (%d/%d) has negative in-flight URLs",
                                _partition, _parallelism));
            }
        }

        Long urlHash = url.makeKey();
        if (_archivedUrls.contains(urlHash)) {
            // It's been archived, ignore...

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("UrlDBFunction ({}/{}) ignoring archived URL '{}'",
                                _partition, _parallelism, url);
            }

            // If the state is unfetched, we're all good, but if not then that's a logical error
            // as we shouldn't be emitting URLs that are archived.
            if (newStatus != FetchStatus.UNFETCHED) {
                throw new RuntimeException(String.format(
                        "UrlDBFunction (%d/%d) got archived URL %s with active status %s",
                        _partition, _parallelism, url, newStatus));
            }
        } else {
            CrawlStateUrl stateUrl = _activeUrls.get(urlHash);
            if (stateUrl == null) {

                // We've never seen this URL before.
                // Better be unfetched.
                if (newStatus != FetchStatus.UNFETCHED) {
                    throw new RuntimeException(String.format(
                            "UrlDBFunction (%d/%d) got new URL '%s' with active status %s",
                            _partition, _parallelism, url, newStatus));
                }

                CounterUtils.increment(getRuntimeContext(), FetchStatus.UNFETCHED);

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("UrlDBFunction ({}/{}) adding new URL '{}' to state",
                                    _partition, _parallelism, url);
                }

                // TODO need to copy URL if object reuse enabled?
                _activeUrls.put(urlHash, url);

                int numActiveUrls = _numActiveUrls.value();
                _activeUrlsIndex.put(numActiveUrls, urlHash);
                _numActiveUrls.update(numActiveUrls + 1);
            } else {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("UrlDBFunction ({}/{}) needs to merge incoming URL '{}' with '{}' (hash {})",
                            _partition, _parallelism, url, stateUrl, urlHash);
                }

                FetchStatus oldStatus = stateUrl.getStatus();
                if (mergeUrls(stateUrl, url)) {
                    _activeUrls.put(urlHash, stateUrl);

                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("UrlDBFunction (({}/{}) updated state of URL '{}' (hash {})",
                                _partition, _parallelism, stateUrl, urlHash);
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
