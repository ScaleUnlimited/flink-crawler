package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.metrics.CrawlerMetrics;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.UrlType;

/**
 * We maintain a set of unique domains that we've seen as our state.
 * 
 * We continuously iterate over this set, generating a special CrawlStateUrl that will trigger loading of the fetch
 * queue by the (downstream) UrlDbFunction as needed. This hack is required because Flink doesn't (yet) support
 * iterating over all keys stored in a state backend.
 * 
 * We implement the ListCheckpointed<String> interface, which lets us checkpoint our state as a list of strings (PLDs).
 *
 */
@SuppressWarnings("serial")
public class DomainDBFunction extends BaseFlatMapFunction<CrawlStateUrl, CrawlStateUrl>
        implements ListCheckpointed<String> {
    static final Logger LOGGER = LoggerFactory.getLogger(DomainDBFunction.class);

    private static int DOMAINS_PER_TICKLE = 100;

    // Sorted list of PLDs, for state.
    private transient List<String> _domains;

    // Unsorted list of domain tickler URLs that we
    // iterate over.
    private transient List<CrawlStateUrl> _urls;
    private transient int _domainIndex;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        _domains = new ArrayList<>();
        _urls = new ArrayList<>();
        _domainIndex = 0;

        RuntimeContext context = getRuntimeContext();

        context.getMetricGroup().gauge(CrawlerMetrics.GAUGE_UNIQUE_PLDS.toString(),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return _domains.size();
                    }
                });

    }

    @Override
    public void flatMap(CrawlStateUrl url, Collector<CrawlStateUrl> collector) throws Exception {
        // Emit the URL we were passed.
        collector.collect(url);

        String domain = url.getPld();
        if (url.getUrlType() == UrlType.REGULAR) {
            processRegularUrl(domain, collector);
        } else if (url.getUrlType() == UrlType.TICKLER) {
            processTicklerUrl(collector);
        }
    }

    private void processTicklerUrl(Collector<CrawlStateUrl> collector) {
        if (_domains.isEmpty()) {
            return;
        }

        // Emit a new "domain tickler" URL type for some number of
        // domains in our list, as we walk through the list.
        int startingIndex = _domainIndex;
        for (int i = 0; i < DOMAINS_PER_TICKLE; i++) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Emitting domain tickler for index " + _domainIndex);
            }

            collector.collect(_urls.get(_domainIndex));
            _domainIndex += 1;

            // Wrap around
            if (_domainIndex >= _urls.size()) {
                _domainIndex = 0;
            }

            if (_domainIndex == startingIndex) {
                // Wrapped around, because we have less than our target number of domains in the list.
                break;
            }
        }
    }

    private void processRegularUrl(String domain, Collector<CrawlStateUrl> collector) {
        int position = Collections.binarySearch(_domains, domain);
        if (position < 0) {
            try {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(String.format("Adding domain '%s' to tickler list", domain));
                }

                CrawlStateUrl url = CrawlStateUrl.makeDomainUrl(domain);
                int index = -position - 1;
                _domains.add(index, domain);
                _urls.add(index, url);
            } catch (MalformedURLException e) {
                LOGGER.error("Got invalid domain name: " + domain);
            }
        }

    }

    @Override
    public List<String> snapshotState(long checkpointId, long timestamp) throws Exception {
        LOGGER.debug(String.format("Checkpointing DomainDBFunction (id %d at %d)", checkpointId,
                timestamp));

        return _domains;
    }

    @Override
    public void restoreState(List<String> state) throws Exception {
        LOGGER.debug(
                String.format("Restoring DomainDBFunction state with %d entries", state.size()));

        if (_domains == null) {
            LOGGER.warn("Restoring DomainDBFunction state but _domains is null!");
            _domains = new ArrayList<>(state);
        } else {
            _domains.clear();
            _domains.addAll(state);
        }

        Collections.sort(_domains);
        _domainIndex = 0;

        // TODO verify no duplicates?

        _urls = new ArrayList<>(_domains.size());
        for (String domain : _domains) {
            _urls.add(CrawlStateUrl.makeDomainUrl(domain));
        }
    }

}
