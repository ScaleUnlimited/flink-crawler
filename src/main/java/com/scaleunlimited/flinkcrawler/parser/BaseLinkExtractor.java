package com.scaleunlimited.flinkcrawler.parser;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.scaleunlimited.flinkcrawler.config.ParserPolicy;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;


@SuppressWarnings("serial")
public abstract class BaseLinkExtractor extends DefaultHandler implements Serializable {
    static final Logger LOGGER = LoggerFactory.getLogger(BaseLinkExtractor.class);

    public static final Set<String> DEFAULT_LINK_TAGS =
        new HashSet<String>() {{
            add("a");
        }};
    
    public static final Set<String> ALL_LINK_TAGS =
        new HashSet<String>() {{
            add("a");
            add("img");
            add("frame");
            add("iframe");
            add("link");
            add("area");
            add("input");
            add("bgsound");
            add("object");
            add("blockquote");
            add("q");
            add("ins");
            add("del");
            add("embed");
        }};
        
    public static final Set<String> DEFAULT_LINK_ATTRIBUTE_TYPES =
        new HashSet<String>() {{
            add("href");
        }};
            
    public static final Set<String> ALL_LINK_ATTRIBUTE_TYPES =
        new HashSet<String>() {{
            add("href");
            add("src");
            add("data");
            add("cite");
        }};

    protected String _inAnchorTag;        
    protected String _curUrl;
    protected String _curRelAttributes;
    protected StringBuilder _curAnchor = new StringBuilder();
    protected Set<String> _linkTags = DEFAULT_LINK_TAGS;
    protected Set<String> _linkAttributeTypes = DEFAULT_LINK_ATTRIBUTE_TYPES;

    /**
     * @param linkTags to collect {@link ExtractedUrl}s from
     * (defaults to {@link BaseLinkExtractor#DEFAULT_LINK_TAGS})
     * <BR><BR><B>Note:</B> There is no need to construct your own
     * {@link SimpleLinkExtractor} simply to control the set of link tags
     * it processes. Instead, provide this set of link tags to
     * {@link ParserPolicy}.
     */
    public void setLinkTags(Set<String> linkTags) {
        _linkTags = linkTags;
    }

    public Set<String> getLinkTags() {
        return _linkTags;
    }
    
    /**
     * @param linkAttributeTypes to collect {@link ExtractedUrl}s from
     * (defaults to {@link BaseLinkExtractor#DEFAULT_LINK_ATTRIBUTE_TYPES})
     * <BR><BR><B>Note:</B> There is no need to construct your own
     * {@link SimpleLinkExtractor} simply to control the set of link attributes
     * it processes. Instead, provide this set of attributes to
     * {@link ParserPolicy}.
     */
    public void setLinkAttributeTypes(Set<String> linkAttributeTypes) {
        _linkAttributeTypes = linkAttributeTypes;
    }

    public Set<String> getLinkAttributeTypes() {
        return _linkAttributeTypes;
    }
    
    public void reset() {
        _inAnchorTag = null;
    }
    
    public void addLink(ExtractedUrl link) {};
    
    public abstract ExtractedUrl[] getLinks();
    
    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);

        String tag = localName.toLowerCase();
        
        if ((_inAnchorTag == null) && _linkTags.contains(tag)) {
            for (String linkAttributeType : _linkAttributeTypes) {
                String attrValue = attributes.getValue(linkAttributeType);
                if (attrValue != null) {
                    _curUrl = attrValue;
                    _curRelAttributes = attributes.getValue("rel");
                    _inAnchorTag = tag;
                    _curAnchor.setLength(0);
                }
            }
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        
        if (_inAnchorTag != null) {
            _curAnchor.append(ch, start, length);
        }
    }

    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        super.endElement(uri, localName, name);

        if (localName.equalsIgnoreCase(_inAnchorTag)) {
        	addLink(new ExtractedUrl(_curUrl, _curAnchor.toString(), _curRelAttributes, RawUrl.DEFAULT_SCORE));
        	_inAnchorTag = null;
        }
    }

}
