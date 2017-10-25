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
public class WebGraphFetcher extends BaseHttpFetcher {

	private static final String TEMPLATE = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\"\n"
        + "\"http://www.w3.org/TR/html4/loose.dtd\">\n"
        + "<html lang=\"en\">\n<head>\n"
        + "\t<meta http-equiv=\"content-type\" content=\"text/html; charset=utf-8\">\n"
        + "\t<title>Synthetic page - score = %f</title>\n</head>\n"
        + "<body>\n<ul>%s</ul>\n</body>\n</html>";
	
	private static final String OUTLINK = "<li><a href=\"%s\">outlink %d</a></li>\n";

	private static final String HTML_MIME_TYPE = "text/html";

	private static final Charset UTF_8 = Charset.forName("UTF-8");
	
	private BaseWebGraph _graph;
	
	public static class WebGraphFetcherBuilder extends BaseHttpFetcherBuilder {
		private WebGraphFetcher _fetcher;

		public WebGraphFetcherBuilder(WebGraphFetcher fetcher) {
			super(fetcher.getMaxThreads(), fetcher.getUserAgent());
			_fetcher = fetcher;
		}

		@Override
		public BaseHttpFetcher build() {
			return _fetcher;
		}

	}
	
	public WebGraphFetcher(BaseWebGraph graph) {
		super(1, new UserAgent("WebGraphFetcher", "flink-crawler@scaleunlimited.com", "http://www.scaleunlimited.com"));
		
		_graph = graph;
	}
	
	@Override
	public FetchedResult get(String urlToFetch, Payload payload) throws BaseFetchException {
		if (!_graph.hasPage(urlToFetch)) {
            return new FetchedResult(
					urlToFetch, 
					urlToFetch, 
					System.currentTimeMillis(), 
					new Headers(),
					new byte[0],
					"text/plain",
					0,
					null,
					urlToFetch,
					0,
					"127.0.0.1",
					HttpStatus.SC_NOT_FOUND,
					"");
		} else if (!isValidMimeType(HTML_MIME_TYPE)) {
            throw new AbortedFetchException(urlToFetch, "Invalid mime-type: " + HTML_MIME_TYPE, AbortedFetchReason.INVALID_MIMETYPE);
		} else {
			int outlinkIndex = 1;
			StringBuilder linksList = new StringBuilder();
			Iterator<String> outlinksIter = _graph.getChildren(urlToFetch);
			while (outlinksIter.hasNext()) {
				String outlink = outlinksIter.next();
				if (!outlink.startsWith("http")) {
					outlink = "http://" + outlink;
				}
				
				linksList.append(String.format(OUTLINK, outlink, outlinkIndex++));
			}
			
			String contentAsStr = String.format(TEMPLATE, _graph.getScore(urlToFetch), linksList);
			return new FetchedResult(
					urlToFetch, 
					urlToFetch, 
					System.currentTimeMillis(), 
					new Headers(),
					contentAsStr.getBytes(UTF_8), 
					HTML_MIME_TYPE, 
					DEFAULT_MIN_RESPONSE_RATE, 
					null, 
					urlToFetch, 
					0, 
					"127.0.0.1", 
					HttpStatus.SC_OK, 
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
