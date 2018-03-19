package com.scaleunlimited.flinkcrawler.parser;

import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.ParsedUrl;

public class ParserResult {

    ParsedUrl _parsedUrl;
    ExtractedUrl[] _extractedUrls;

    public ParserResult(ParsedUrl parsedUrl, ExtractedUrl[] extractedUrls) {
        _parsedUrl = parsedUrl;
        _extractedUrls = extractedUrls;
    }

    public ParsedUrl getParsedUrl() {
        return _parsedUrl;
    }

    public ExtractedUrl[] getExtractedUrls() {
        return _extractedUrls;
    }
}
