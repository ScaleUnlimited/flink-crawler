package com.scaleunlimited.flinkcrawler.functions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;
import com.scaleunlimited.flinkcrawler.metrics.CrawlerMetrics;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.utils.ExceptionUtils;

import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.BaseHttpFetcher;

@SuppressWarnings("serial")
public class FetchUrlsFunction
        extends BaseAsyncFunction<FetchUrl, FetchResultUrl> {
    static final Logger LOGGER = LoggerFactory.getLogger(FetchUrlsFunction.class);

    private static final int FETCH_RATE_WINDOW_SIZE = 30;

    private BaseHttpFetcherBuilder _fetcherBuilder;
    private BaseHttpFetcher _fetcher;

    // TODO use native String->long map
    private transient Map<String, Long> _nextFetch;

    private transient TimedCounter _fetchCounts;

    /**
     * Returns a Tuple2 of the CrawlStateUrl and FetchedUrl. In the case of an error while fetching the FetchedUrl is
     * set to null.
     * 
     * @param fetcherBuider
     */
    public FetchUrlsFunction(BaseHttpFetcherBuilder fetcherBuilder) {
        super(fetcherBuilder.getMaxSimultaneousRequests(),
                fetcherBuilder.getFetchDurationTimeoutInSeconds());

        _fetcherBuilder = fetcherBuilder;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        getRuntimeContext().getMetricGroup().gauge(
                CrawlerMetrics.GAUGE_URLS_CURRENTLY_BEING_FETCHED.toString(), new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        if (_executor != null) {
                            return _executor.getActiveCount();
                        }
                        return 0;
                    }
                });

        _fetchCounts = new TimedCounter(FETCH_RATE_WINDOW_SIZE);
        getRuntimeContext().getMetricGroup().gauge(
                CrawlerMetrics.GAUGE_URLS_FETCHED_PER_SECOND.toString(), new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return _fetchCounts.getTotalCounts() / FETCH_RATE_WINDOW_SIZE;
                    }
                });

        _fetcher = _fetcherBuilder.build();
        _nextFetch = new HashMap<>();
    }

    @Override
    public void asyncInvoke(FetchUrl url, ResultFuture<FetchResultUrl> future)
            throws Exception {
        record(this.getClass(), url);

        final String domainKey = url.getUrlWithoutPath();
        Long nextFetchTime = _nextFetch.get(domainKey);
        long currentTime = System.currentTimeMillis();
        if ((nextFetchTime != null) && (currentTime < nextFetchTime)) {
            LOGGER.debug("Skipping (crawl-delay) " + url);
            future.complete(skipUrl(url, nextFetchTime));
            return;
        } else {
            _nextFetch.put(domainKey, currentTime + url.getCrawlDelay());
        }

        LOGGER.debug("Queueing for fetch: " + url);
        _executor.execute(new Runnable() {

            @Override
            public void run() {
                LOGGER.debug("Fetching " + url);

                try {
                    FetchedResult result = _fetcher.get(url.getUrl(), null);
                    FetchStatus fetchStatus = (result.getStatusCode() == HttpStatus.SC_OK) ?
                                FetchStatus.FETCHED
                            :   ExceptionUtils
                                    .mapHttpStatusToFetchStatus(result.getStatusCode());
                    FetchResultUrl fetchedUrl = new FetchResultUrl(url, fetchStatus, result.getFetchTime(),
                            result.getFetchedUrl(), result.getHeaders(), result.getContent(),
                            result.getContentType(), result.getResponseRate());

                    _fetchCounts.increment();

                    // If we got an error, put null in for fetchedUrl so we don't try to process it downstream.
                    if (result.getStatusCode() != HttpStatus.SC_OK) {
                        String msg = String.format("Failed to fetch '%s' (%d)",
                                result.getFetchedUrl(), result.getStatusCode());
                        if (result.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                            LOGGER.trace(msg);
                        } else {
                            LOGGER.debug(msg);
                        }

                        // TODO set next fetch time to something valid, based on the error
                        LOGGER.trace("Forwarded failed URL to update status: '{}'",
                                result.getFetchedUrl());
                    } else {
                        // TODO set next fetch time to something valid.
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("Fetched {} bytes from '{}'",
                                    result.getContentLength(), result.getFetchedUrl());
                            
                            LOGGER.trace("Forwarded fetched URL to be parsed: '{}'",
                                    result.getFetchedUrl());
                        }
                    }
                    
                    future.complete(Collections.singleton(fetchedUrl));
                } catch (Exception e) {
                    LOGGER.trace("Failed to fetch '{}' due to {}", url, e.getMessage());

                    if (e instanceof BaseFetchException) {
                        future.complete(Collections.singleton(new FetchResultUrl(url, ExceptionUtils.mapExceptionToFetchStatus(e),
                                System.currentTimeMillis())));
                        LOGGER.trace("Forwarded exception URL to update status: '{}'", url);
                    } else {
                        throw new RuntimeException("Exception fetching " + url, e);
                    }
                } catch (Throwable t) {
                    LOGGER.error(String.format("Serious error trying to fetch '%s' due to %s", url,
                            t.getMessage()), t);
                    throw new RuntimeException(t);
                }
            }
        });
    }

    private Collection<FetchResultUrl> skipUrl(FetchUrl url, Long nextFetchTime) {
        FetchResultUrl fetchResultUrl = new FetchResultUrl(url, FetchStatus.SKIPPED_CRAWLDELAY,
                System.currentTimeMillis());
        fetchResultUrl.setNextFetchTime(nextFetchTime);
        return Collections.singleton(fetchResultUrl);
    }

    protected static class TimedCounter {

        private int[] _countsPerSecond;
        private long _lastTimeInSeconds;
        private int _numSeconds;

        public TimedCounter(int numSeconds) {
            _numSeconds = numSeconds;
            _countsPerSecond = new int[_numSeconds];
            _lastTimeInSeconds = System.currentTimeMillis() / 1000L;
        }

        public void increment() {
            increment(System.currentTimeMillis());
        }

        public void increment(long timeInMS) {
            synchronized (_countsPerSecond) {
                long timeInSeconds = timeInMS / 1000L;
                int deltaSeconds = (int) (timeInSeconds - _lastTimeInSeconds);
                if (deltaSeconds < 0) {
                    throw new RuntimeException("Time can't go backwards");
                }

                if (deltaSeconds == 0) {
                    // No shift or fill
                } else if (deltaSeconds >= _numSeconds) {
                    // No need to shift, just clear everything and then increment.
                    Arrays.fill(_countsPerSecond, 0);
                } else {
                    // Shift values, clear new counts.
                    System.arraycopy(_countsPerSecond, deltaSeconds, _countsPerSecond, 0,
                            _numSeconds - deltaSeconds);
                    Arrays.fill(_countsPerSecond, _numSeconds - deltaSeconds, _numSeconds, 0);
                }

                _lastTimeInSeconds = timeInSeconds;
                _countsPerSecond[_numSeconds - 1]++;
            }
        }

        public int getTotalCounts() {
            synchronized (_countsPerSecond) {
                int result = 0;
                for (int i = 0; i < _numSeconds; i++) {
                    result += _countsPerSecond[i];
                }

                return result;
            }
        }

        protected int[] getCountsPerSecond() {
            return _countsPerSecond;
        }
    }

}
