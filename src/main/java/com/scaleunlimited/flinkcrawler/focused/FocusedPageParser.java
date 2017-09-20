package com.scaleunlimited.flinkcrawler.focused;

import com.scaleunlimited.flinkcrawler.parser.ParserResult;
import com.scaleunlimited.flinkcrawler.parser.SimplePageParser;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchedUrl;

@SuppressWarnings("serial")
public class FocusedPageParser extends SimplePageParser {

	private final BasePageScorer _pageScorer;
	
	public FocusedPageParser(BasePageScorer pageScorer) {
		super();
		
		_pageScorer = pageScorer;
	}
	
	@Override
	public ParserResult parse(FetchedUrl fetchedUrl) throws Exception {
		ParserResult result = super.parse(fetchedUrl);
		float score = _pageScorer.score(result);
		result.getParsedUrl().setScore(score);
		
		// Set the score of each outlink to its fraction of the page score.
		ExtractedUrl[] outlinks = result.getExtractedUrls();
		for (ExtractedUrl outlink : outlinks) {
			outlink.setScore(score/outlinks.length);
		}
		
		return result;
	}
}
