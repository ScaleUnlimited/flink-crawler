package com.scaleunlimited.flinkcrawler.fetcher;

import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.http.HttpStatus;

import com.scaleunlimited.flinkcrawler.webgraph.BaseWebGraph;

import crawlercommons.fetcher.AbortedFetchException;
import crawlercommons.fetcher.AbortedFetchReason;
import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.util.Headers;

@SuppressWarnings("serial")
public class SiteMapGraphFetcher extends BaseHttpFetcher {

    private static final String SITEMAP_ENTRY = "%s\n";

    private static final String TEXT_MIME_TYPE = "text/plain";

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private BaseWebGraph _graph;

    public static class SiteMapGraphFetcherBuilder extends BaseHttpFetcherBuilder {
        private SiteMapGraphFetcher _fetcher;

        public SiteMapGraphFetcherBuilder(SiteMapGraphFetcher fetcher) {
            super(fetcher.getMaxThreads(), fetcher.getUserAgent());
            _fetcher = fetcher;
        }

        @Override
        public BaseHttpFetcher build() {
            return _fetcher;
        }

    }

    public SiteMapGraphFetcher(BaseWebGraph graph) {
        super(1, new UserAgent("SiteMapGraphFetcher", "flink-crawler@scaleunlimited.com",
                "http://www.scaleunlimited.com"));

        _graph = graph;
    }

    @Override
    public FetchedResult get(String urlToFetch, Payload payload) throws BaseFetchException {
        Iterator<String> outlinksIter = _graph.getChildren(urlToFetch);

        if (outlinksIter == null) {
            return new FetchedResult(urlToFetch, urlToFetch, System.currentTimeMillis(),
                    new Headers(), new byte[0], "text/plain", 0, null, urlToFetch, 0, "127.0.0.1",
                    HttpStatus.SC_NOT_FOUND, "");
        } else if (!isValidMimeType(TEXT_MIME_TYPE)) {
            throw new AbortedFetchException(urlToFetch, "Invalid mime-type: " + TEXT_MIME_TYPE,
                    AbortedFetchReason.INVALID_MIMETYPE);
        } else {
            int outlinkIndex = 1;
            StringBuilder content = new StringBuilder();
            while (outlinksIter.hasNext()) {
                String outlink = outlinksIter.next();
                if (!outlink.startsWith("http")) {
                    outlink = "http://" + outlink;
                }

                content.append(String.format(SITEMAP_ENTRY, outlink, outlinkIndex++));
            }

            String contentAsStr = content.toString();
            return new FetchedResult(urlToFetch, urlToFetch, System.currentTimeMillis(),
                    new Headers(), contentAsStr.getBytes(UTF_8), TEXT_MIME_TYPE,
                    DEFAULT_MIN_RESPONSE_RATE, null, urlToFetch, 0, "127.0.0.1", HttpStatus.SC_OK,
                    "");
        }
    }

    private boolean isValidMimeType(String htmlMimeType) {
        return _validMimeTypes.isEmpty() || _validMimeTypes.contains(htmlMimeType);
    }

    @Override
    public void abort() {
        // nothing to abort
    }

}
