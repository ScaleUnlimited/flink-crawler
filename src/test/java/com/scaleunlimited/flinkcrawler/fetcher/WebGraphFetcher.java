package com.scaleunlimited.flinkcrawler.fetcher;

import java.util.List;
import java.util.Map;

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

	private BaseWebGraph _graph;
	
	public WebGraphFetcher(BaseWebGraph graph) {
		_graph = graph;
	}
	
	@Override
	public byte[] fetch(FetchUrl url, Map<String, List<String>> headers) {
		// TODO Auto-generated method stub
		return null;
	}

}
