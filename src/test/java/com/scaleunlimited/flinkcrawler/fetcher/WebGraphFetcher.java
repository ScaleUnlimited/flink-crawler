package com.scaleunlimited.flinkcrawler.fetcher;

import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.http.HttpStatus;
import org.apache.tika.metadata.Metadata;

import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;
import com.scaleunlimited.flinkcrawler.webgraph.BaseWebGraph;

@SuppressWarnings("serial")
public class WebGraphFetcher extends BaseFetcher {

	private static final String TEMPLATE = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\"\n"
        + "\"http://www.w3.org/TR/html4/loose.dtd\">\n"
        + "<html lang=\"en\">\n<head>\n"
        + "\t<meta http-equiv=\"content-type\" content=\"text/html; charset=utf-8\">\n"
        + "\t<title>Synthetic page</title>\n</head>\n"
        + "<body>\n<ul>%s</ul>\n</body>\n</html>";
	
	private static final String OUTLINK = "<li><a href=\"%s\">outlink %d</a></li>\n";

	private static final String HTML_MIME_TYPE = "text/html";

	private static final Charset UTF_8 = Charset.forName("UTF-8");
	
	private BaseWebGraph _graph;
	
	public WebGraphFetcher(BaseWebGraph graph) {
		super(new UserAgent("WebGraphFetcher", "flink-crawler@scaleunlimited.com", "http://www.scaleunlimited.com"));
		
		_graph = graph;
	}
	
	@Override
	public FetchedResult get(FetchUrl url) throws BaseFetchException {
		String urlToFetch = url.getUrl();
		Iterator<String> outlinksIter = _graph.getChildren(urlToFetch);
		if ((outlinksIter == null) && urlToFetch.startsWith("http")) {
			String rawUrl = urlToFetch.replaceFirst("^(http|https)://", "");
			outlinksIter = _graph.getChildren(rawUrl);
		}
		
		if (outlinksIter == null) {
            throw new HttpFetchException(urlToFetch, "Error fetching " + urlToFetch, HttpStatus.SC_NOT_FOUND, new Metadata());
		} else if (!isValidMimeType(HTML_MIME_TYPE)) {
            throw new AbortedFetchException(urlToFetch, "Invalid mime-type: " + HTML_MIME_TYPE, AbortedFetchReason.INVALID_MIMETYPE);
		} else {
			int outlinkIndex = 1;
			StringBuilder linksList = new StringBuilder();
			while (outlinksIter.hasNext()) {
				String outlink = outlinksIter.next();
				if (!outlink.startsWith("http")) {
					outlink = "http://" + outlink;
				}
				
				linksList.append(String.format(OUTLINK, outlink, outlinkIndex++));
			}
			
			String contentAsStr = String.format(TEMPLATE, linksList);
			return new FetchedResult(urlToFetch, null, System.currentTimeMillis(), new Metadata(), contentAsStr.getBytes(UTF_8), HTML_MIME_TYPE, DEFAULT_MIN_RESPONSE_RATE, null, 0, HttpStatus.SC_OK);
		}
	}

	@Override
	public void abort() {
		// nothing to abort
	}

}
