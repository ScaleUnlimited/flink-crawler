package com.scaleunlimited.flinkcrawler.functions;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.UrlType;

/**
 * We maintain a set of unique domains that we've seen as our state.
 * 
 * We continuously iterate over this set, generating a special CrawlStateUrl
 * that will trigger loading of the fetch queue by the (downstream) CrawlDbFunction
 * as needed. This hack is required because Flink doesn't (yet) support iterating
 * over all keys stored in a state backend.
 *
 */
@SuppressWarnings("serial")
public class DomainTicklerFunction extends BaseFlatMapFunction<CrawlStateUrl, CrawlStateUrl> implements ListCheckpointed<String> {
    static final Logger LOGGER = LoggerFactory.getLogger(DomainTicklerFunction.class);

	private static int DOMAINS_PER_TICKLE = 100;
	
	private List<String> _domains;
	private List<CrawlStateUrl> _urls;
	private int _domainIndex;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		
		_domains = new ArrayList<>();
		_urls = new ArrayList<>();
	}
	
	@Override
	public void flatMap(CrawlStateUrl url, Collector<CrawlStateUrl> collector) throws Exception {
		// Emit the url we were passed.
		collector.collect(url);
		
		if (url.getUrlType() == UrlType.REGULAR) {
		    if (LOGGER.isTraceEnabled()) {
		        LOGGER.trace("Adding domain tickler list: " + url.getPld());
		    }
		    
			addDomain(url.getPld());
		} else if ((url.getUrlType() == UrlType.TICKLER) && !_domains.isEmpty()) {
			// If it's a tickler, emit a new "domain tickler" URL type for some number of
			// domains in our list, as we walk through the list.
			int startingIndex = _domainIndex;
			for (int i = 0; i < DOMAINS_PER_TICKLE; i++) {
				if (_domainIndex >= _urls.size()) {
					_domainIndex = 0;
				}
				
				if (LOGGER.isTraceEnabled()) {
				    LOGGER.trace("Emitting domain tickler for index " + _domainIndex);
				}
				
				collector.collect(_urls.get(_domainIndex));
				_domainIndex += 1;
				
				if (_domainIndex == startingIndex) {
					// Wrapped around (we have less than our target number of domains in the list)
					break;
				}
			}
		}
	}

	private void addDomain(String domain) {
		int position = Collections.binarySearch(_domains, domain);
		if (position < 0) {
            try {
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
		return _domains;
	}

	@Override
	public void restoreState(List<String> state) throws Exception {
		_domains.clear();
		_domains.addAll(state);
		
		Collections.sort(_domains);

		// TODO verify no duplicates?
		
		_domainIndex = 0;
	}

}
