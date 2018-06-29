package com.scaleunlimited.flinkcrawler.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.IdentityHtmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.config.ParserPolicy;
import com.scaleunlimited.flinkcrawler.focused.AllEqualPageScorer;
import com.scaleunlimited.flinkcrawler.focused.BasePageScorer;
import com.scaleunlimited.flinkcrawler.metrics.CrawlerMetrics;
import com.scaleunlimited.flinkcrawler.pojos.ExtractedUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchResultUrl;
import com.scaleunlimited.flinkcrawler.utils.IoUtils;

@SuppressWarnings("serial")
public class SimplePageParser extends BasePageParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimplePageParser.class);

    private boolean _extractLanguage = true;
    protected BaseContentExtractor _contentExtractor;
    protected BaseLinkExtractor _linkExtractor;
    protected ParseContext _parseContext;

    private transient Parser _parser;

    public SimplePageParser() {
        this(new ParserPolicy(), new AllEqualPageScorer());
    }

    public SimplePageParser(ParserPolicy parserPolicy, BasePageScorer pageScorer) {
        this(new SimpleContentExtractor(), new SimpleLinkExtractor(), parserPolicy, null,
                pageScorer);
    }

    /**
     * @param contentExtractor
     *            to use instead of new {@link SimpleContentExtractor}()
     * @param linkExtractor
     *            to use instead of new {@link SimpleLinkExtractor}()
     * @param parserPolicy
     *            to customize operation of the parser <BR>
     * <BR>
     *            <B>Note:</B> There is no need to construct your own {@link SimpleLinkExtractor}
     *            simply to control the set of link tags and attributes it processes. Instead, use
     *            {@link ParserPolicy#setLinkTags} and {@link ParserPolicy#setLinkAttributeTypes},
     *            and then pass this policy to {@link SimplePageParser#SimpleParser(ParserPolicy)}.
     * @param pageScorer
     *            to score importance of page (priority of its outlinks)
     */
    public SimplePageParser(BaseContentExtractor contentExtractor, BaseLinkExtractor linkExtractor,
            ParserPolicy parserPolicy, BasePageScorer pageScorer) {
        this(contentExtractor, linkExtractor, parserPolicy, null, pageScorer);
    }

    /**
     * @param parserPolicy
     *            to customize operation of the parser
     * @param pageScorer
     *            to score importance of page (priority of its outlinks)
     * @param includeMarkup
     *            true if output should be raw HTML, versus extracted text <BR>
     * <BR>
     *            <B>Note:</B> There is no need to construct your own {@link SimpleLinkExtractor}
     *            simply to control the set of link tags and attributes it processes. Instead, use
     *            {@link ParserPolicy#setLinkTags} and {@link ParserPolicy#setLinkAttributeTypes},
     *            and then pass this policy to {@link SimplePageParser#SimpleParser(ParserPolicy)}.
     */
    public SimplePageParser(ParserPolicy parserPolicy, BasePageScorer pageScorer,
            boolean includeMarkup) {
        this(includeMarkup ? new HtmlContentExtractor() : new SimpleContentExtractor(),
                new SimpleLinkExtractor(), parserPolicy, pageScorer, includeMarkup);
    }

    /**
     * @param parserPolicy
     *            to customize operation of the parser
     * @param pageScorer
     *            to score importance of page (priority of its outlinks)
     * @param includeMarkup
     *            true if output should be raw HTML, versus extracted text <BR>
     * <BR>
     *            <B>Note:</B> There is no need to construct your own {@link SimpleLinkExtractor}
     *            simply to control the set of link tags and attributes it processes. Instead, use
     *            {@link ParserPolicy#setLinkTags} and {@link ParserPolicy#setLinkAttributeTypes},
     *            and then pass this policy to {@link SimplePageParser#SimpleParser(ParserPolicy)}.
     */
    public SimplePageParser(BaseContentExtractor contentExtractor, BaseLinkExtractor linkExtractor,
            ParserPolicy parserPolicy, BasePageScorer pageScorer, boolean includeMarkup) {
        super(parserPolicy, pageScorer);

        _contentExtractor = contentExtractor;
        _linkExtractor = linkExtractor;

        if (includeMarkup) {
            _parseContext = new ParseContext();
            _parseContext.set(HtmlMapper.class, IdentityHtmlMapper.INSTANCE);
        }
    }

    /**
     * @param contentExtractor
     *            to use instead of new {@link SimpleContentExtractor}()
     * @param linkExtractor
     *            to use instead of new {@link SimpleLinkExtractor}()
     * @param parserPolicy
     *            to customize operation of the parser
     * @param parseContext
     *            used to pass context info to the parser <BR>
     * <BR>
     *            <B>Note:</B> There is no need to construct your own {@link SimpleLinkExtractor}
     *            simply to control the set of link tags and attributes it processes. Instead, use
     *            {@link ParserPolicy#setLinkTags} and {@link ParserPolicy#setLinkAttributeTypes},
     *            and then pass this policy to {@link SimplePageParser#SimpleParser(ParserPolicy)}.
     * @param pageScorer
     *            to score importance of page (priority of its outlinks)
     */
    public SimplePageParser(BaseContentExtractor contentExtractor, BaseLinkExtractor linkExtractor,
            ParserPolicy parserPolicy, ParseContext parseContext, BasePageScorer pageScorer) {
        super(parserPolicy, pageScorer);

        _contentExtractor = contentExtractor;
        _linkExtractor = linkExtractor;
        _parseContext = parseContext;
    }

    @Override
    public void open(RuntimeContext context) throws Exception {
        super.open(context);

        _parser = new AutoDetectParser();
        _linkExtractor.setLinkTags(getParserPolicy().getLinkTags());
        _linkExtractor.setLinkAttributeTypes(getParserPolicy().getLinkAttributeTypes());
    }

    public void setExtractLanguage(boolean extractLanguage) {
        _extractLanguage = extractLanguage;
    }

    public boolean isExtractLanguage() {
        return _extractLanguage;
    }

    @Override
    public ParserResult parse(FetchResultUrl fetchedUrl) throws Exception {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Parsing '{}'", fetchedUrl.getFetchedUrl());
        }

        // Provide clues to the parser about the format of the content.
        Metadata metadata = new Metadata();
        metadata.add(Metadata.RESOURCE_NAME_KEY, fetchedUrl.getFetchedUrl());
        metadata.add(Metadata.CONTENT_TYPE, fetchedUrl.getContentType());
        String charset = getCharset(fetchedUrl);
        metadata.add(Metadata.CONTENT_LANGUAGE, getLanguage(fetchedUrl, charset));

        byte[] content = fetchedUrl.getContent();
        InputStream is = new ByteArrayInputStream(content, 0, content.length);

        try {
            URL baseUrl = getContentLocation(fetchedUrl);
            metadata.add(Metadata.CONTENT_LOCATION, baseUrl.toExternalForm());

            _contentExtractor.reset();
            _linkExtractor.reset();

            Callable<ParserResult> c = new TikaCallable(_parser, _contentExtractor, _linkExtractor,
                    is, metadata, isExtractLanguage(), _parseContext);
            FutureTask<ParserResult> task = new FutureTask<ParserResult>(c);
            Thread t = new Thread(task);
            t.start();

            try {
                ParserResult result = task.get(getParserPolicy().getMaxParseDuration(),
                        TimeUnit.MILLISECONDS);
                getAccumulator().increment(CrawlerMetrics.COUNTER_PAGES_PARSED);

                // Score the page itself
                float score = getPageScorer().score(result);
                result.getParsedUrl().setScore(score);

                // Set the score of each outlink to its fraction of the page score.
                ExtractedUrl[] outlinks = result.getExtractedUrls();
                for (ExtractedUrl outlink : outlinks) {
                    outlink.setScore(score / outlinks.length);
                }
                return result;
            } catch (TimeoutException e) {
                task.cancel(true);
                t.interrupt();
                throw e;
            }
        } catch (Exception e) {
            getAccumulator().increment(CrawlerMetrics.COUNTER_PAGES_FAILEDPARSE);
            throw e;
        } finally {
            IoUtils.safeClose(is);
        }
    }

}
