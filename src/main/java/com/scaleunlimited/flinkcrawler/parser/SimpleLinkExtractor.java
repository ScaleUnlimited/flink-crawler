package com.scaleunlimited.flinkcrawler.parser;

import java.util.HashSet;
import java.util.Set;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;

@SuppressWarnings("serial")
public class SimpleLinkExtractor extends BaseLinkExtractor {
    private static final int DEFAULT_MAX_EXTRACTED_LINKS_SIZE = 100;

    private boolean _inHead;
    private boolean _skipLinks;
    private int _maxExtractedLinksSize = DEFAULT_MAX_EXTRACTED_LINKS_SIZE;

    private Set<ExtractedUrl> _extractedUrls = new HashSet<ExtractedUrl>();

    public SimpleLinkExtractor() {
        this(DEFAULT_MAX_EXTRACTED_LINKS_SIZE);
    }

    public SimpleLinkExtractor(int maxExtractedLinksSize) {
        _maxExtractedLinksSize = maxExtractedLinksSize;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws org.xml.sax.SAXException {
        super.startElement(uri, localName, qName, attributes);

        if (!_inHead && localName.equalsIgnoreCase("head")) {
            _inHead = true;
        } else if (_inHead && localName.equalsIgnoreCase("meta")) {
            // See if we have a robots directive
            String attrName = attributes.getValue("name");
            String content = attributes.getValue("content");
            if ((attrName != null) && attrName.equalsIgnoreCase("robots") && (content != null)) {
                String[] directives = content.split(",");
                for (String directive : directives) {
                    directive = directive.trim().toLowerCase();
                    if (directive.equals("none") || directive.equals("nofollow")) {
                        _skipLinks = true;
                        break;
                    }
                }
            }
        }
    };

    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        super.endElement(uri, localName, name);

        if (_inHead && localName.equalsIgnoreCase("head")) {
            _inHead = false;
        }
    }

    @Override
    public void reset() {
        super.reset();
        _extractedUrls.clear();
        _inHead = false;
        _skipLinks = false;
    }

    @Override
    public void addLink(ExtractedUrl link) {
        if (!_skipLinks) {
            if (_extractedUrls.size() <= _maxExtractedLinksSize) {
                _extractedUrls.add(link);
            }
        }
    }

    @Override
    public ExtractedUrl[] getLinks() {
        return _extractedUrls.toArray(new ExtractedUrl[_extractedUrls.size()]);
    }
}