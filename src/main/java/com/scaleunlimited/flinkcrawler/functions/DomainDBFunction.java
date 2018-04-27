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
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.metrics.CrawlerMetrics;
import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;
import com.scaleunlimited.flinkcrawler.pojos.UrlType;
import com.scaleunlimited.flinkcrawler.utils.FlinkUtils;

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
public class DomainDBFunction extends BaseProcessFunction<CrawlStateUrl, CrawlStateUrl>
        implements ListCheckpointed<String> {
    static final Logger LOGGER = LoggerFactory.getLogger(DomainDBFunction.class);

    public static final OutputTag<CrawlStateUrl> DOMAIN_TICKLER_TAG 
        = new OutputTag<CrawlStateUrl>("domain-tickler"){};

    private static int DOMAINS_PER_TICKLE = 100;

    // Sorted list of PLDs, for state.
    private List<String> _domains;

    // Unsorted list of domain tickler URLs that we
    // iterate over, using _domainIndex.
    private List<CrawlStateUrl> _urls;
    private int _domainIndex;

    // HACK!!! When we get a restore state call, we can get domains that should
    // in the state for other operators. So we'll need to emit those via our
    // side output, so that they get distributed properly.
    private List<String> _otherDomains;
    
    public DomainDBFunction() {
        
        // Note we have to set up our state-related variables here,
        // as open() isn't called before restoreState().
        _domains = new ArrayList<>();
        _otherDomains = new ArrayList<>();
        _urls = new ArrayList<>();
        _domainIndex = 0;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

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
    public void processElement(CrawlStateUrl url, Context context, Collector<CrawlStateUrl> collector) throws Exception {
        // Emit the URL we were passed.
        collector.collect(url);

        if (url.getUrlType() == UrlType.REGULAR) {
            addDomainIfNew(url.getPld());
        } else if (url.getUrlType() == UrlType.TICKLER) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("DomainDBFunction ({}/{}) processing tickler URL {}...", _operatorIndex, _parallelism, url);
            }
            
            processTicklerUrl(url, context);
        } else if (url.getUrlType() == UrlType.DOMAIN) {
            // We might get a domain that was in some other operator's state,
            // which is being re-emitted so we can get it here.
            addDomainIfNew(url.getPld());
        }
    }

    private void processTicklerUrl(CrawlStateUrl url, Context context) throws MalformedURLException {
        
        // First see if we have any stashed domains from state restoration
        // that we need to recycle.
        if (!_otherDomains.isEmpty()) {
            for (String otherDomain : _otherDomains) {
                context.output(DOMAIN_TICKLER_TAG, CrawlStateUrl.makeDomainUrl(otherDomain));
            }
            
            _otherDomains.clear();
        }
        
        if (_domains.isEmpty()) {
            // We still need to emit something, so that the iteration doesn't
            // terminate. But we don't want to cause fan-out, so emit a tickler
            // that's the same as what we got, but with a different status, so
            // that we can tell the difference.
            if (url.getStatus() == FetchStatus.UNFETCHED) {
                CrawlStateUrl newUrl = new CrawlStateUrl();
                newUrl.setFrom(url);
                newUrl.setStatus(FetchStatus.FETCHED);
                context.output(DOMAIN_TICKLER_TAG, newUrl);
            }
            
            return;
        }

        // Emit a new "domain tickler" URL type for some number of
        // domains in our list, as we walk through the list.
        int startingIndex = _domainIndex;
        for (int i = 0; i < DOMAINS_PER_TICKLE; i++) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("DomainDBFunction ({}/{}) emitting domain tickler for '{}'", 
                        _operatorIndex, _parallelism, _urls.get(_domainIndex));
            }

            context.output(DOMAIN_TICKLER_TAG, _urls.get(_domainIndex));

            // Wrap around
            _domainIndex += 1;
            if (_domainIndex >= _urls.size()) {
                _domainIndex = 0;
            }

            if (_domainIndex == startingIndex) {
                // Wrapped around, because we have less than our target number of domains in the list.
                break;
            }
        }
    }

    private void addDomainIfNew(String domain) {
        int position = Collections.binarySearch(_domains, domain);
        if (position < 0) {
            try {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("DomainDBFunction ({}/{}) adding domain '{}'", 
                            _operatorIndex, _parallelism, domain);
                }

                CrawlStateUrl url = CrawlStateUrl.makeDomainUrl(domain);
                int index = -position - 1;
                _domains.add(index, domain);
                _urls.add(index, url);
            } catch (MalformedURLException e) {
                LOGGER.error("Got invalid domain name: " + domain);
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("(partition {} of {}) Domain '{}' already in tickler list", _operatorIndex, _parallelism, domain);
            }
        }
    }

    @Override
    public List<String> snapshotState(long checkpointId, long timestamp) throws Exception {
        LOGGER.info("(partition {} of {}) Checkpointing DomainDBFunction (id {} at {})", _operatorIndex, _parallelism, checkpointId, timestamp);

        // FUTURE if we have _otherDomains, add them to _domains?
        return _domains;
    }

    @Override
    public void restoreState(List<String> state) throws Exception {
        LOGGER.info("(partition {} of {}) Restoring DomainDBFunction state with {} entries", _operatorIndex, _parallelism, state.size());

        _domains.clear();
        _domains.addAll(state);
        Collections.sort(_domains);
        
        // Verify that every entry belongs to us, as the state we get is randomly distributed (not keyed)
        // If we get an entry that doesn't belong, then we will want to send that out (iterate on it) so
        // that it gets added to the other operators. Note that we only have to do this if the parallelism
        // is > 1.
        _otherDomains.clear();
        if (_parallelism > 1) {
            for (int i = 0; i < _domains.size(); i++) {
                String domain = _domains.get(i);
                int operatorIndex = FlinkUtils.getOperatorIndexForKey(domain, _maxParallelism, _parallelism);
                if (operatorIndex != _operatorIndex - 1) {
                    _domains.remove(i);
                    i--;
                    
                    // Set up for recycling of domain, so it gets partitioned to the correct operator.
                    _otherDomains.add(domain);
                } else if ((i > 0) && _domains.get(i-1).equals(domain)) {
                    // Sanity check - remove duplicates.
                    _domains.remove(i);
                    i--;
                }
            }
        }
        
        _domainIndex = 0;

        _urls = new ArrayList<>(_domains.size());
        for (String domain : _domains) {
            _urls.add(CrawlStateUrl.makeDomainUrl(domain));
        }
    }


}
