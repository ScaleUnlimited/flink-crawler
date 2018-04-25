package com.scaleunlimited.flinkcrawler.tools;

import org.kohsuke.args4j.Option;

import com.scaleunlimited.flinkcrawler.parser.SimpleLinkExtractor;
import com.scaleunlimited.flinkcrawler.topology.CrawlTopologyBuilder;

import crawlercommons.fetcher.http.SimpleHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

public class CrawlToolOptions {

    private UserAgent _userAgent = null;
    private String _urlsFilename;
    private String _singleDomain;
    private long _forceCrawlDelay = CrawlTool.DO_NOT_FORCE_CRAWL_DELAY;
    private long _defaultCrawlDelayMS = 10 * 1000L;
    private int _maxContentSize = SimpleHttpFetcher.DEFAULT_MAX_CONTENT_SIZE;
    private int _fetchersPerTask = 1;
    private int _parallelism = CrawlTopologyBuilder.DEFAULT_PARALLELISM;
    private String _outputFile = null;
    private boolean _htmlOnly = false;
    private boolean _noLengthen = false;
    private String _checkpointDir = null;
    private int _maxOutlinksPerPage = SimpleLinkExtractor.DEFAULT_MAX_EXTRACTED_LINKS_SIZE;

    private String _cacheDir;
    private String _commonCrawlId = null;

    @Option(name = "-agent", usage = "user agent info, format:'name,email,website'", required = false)
    public void setUserAgent(String agentNameWebsiteEmailString) {
        if (isCommonCrawl()) {
            throw new RuntimeException("user agent not used in common crawl mode");
        }
        String fields[] = agentNameWebsiteEmailString.split(",", 4);
        if (fields.length != 3) {
            throw new RuntimeException(
                    "    Invalid format for user agent (expected 'name,email,website'): "
                            + agentNameWebsiteEmailString);
        }
        String agentName = fields[0];
        String agentEmail = fields[1];
        String agentWebSite = fields[2];
        if (!(agentEmail.contains("@"))) {
            throw new RuntimeException("Invalid email address for user agent: " + agentEmail);
        }
        if (!(agentWebSite.startsWith("http"))) {
            throw new RuntimeException("Invalid web site URL for user agent: " + agentWebSite);
        }
        setUserAgent(new UserAgent(agentName, agentEmail, agentWebSite));
    }

    public void setUserAgent(UserAgent newUserAgent) {
        _userAgent = newUserAgent;
    }

    @Option(name = "-seedurls", usage = "text file containing list of seed urls", required = true)
    public void setSeedUrlsFilename(String urlsFilename) {
        _urlsFilename = urlsFilename;
    }

    @Option(name = "-singledomain", usage = "only fetch URLs within this domain (and its sub-domains)", required = false)
    public void setSingleDomain(String singleDomain) {
        _singleDomain = singleDomain;
    }

    @Option(name = "-forcecrawldelay", usage = "use this crawl delay (ms) even if robots.txt provides something else", required = false)
    public void setForceCrawlDelay(long forceCrawlDelay) {
        _forceCrawlDelay = forceCrawlDelay;
    }

    @Option(name = "-defaultcrawldelay", usage = "use this crawl delay (ms) when robots.txt doesn't provide it", required = false)
    public void setDefaultCrawlDelayMS(long defaultCrawlDelayMS) {
        _defaultCrawlDelayMS = defaultCrawlDelayMS;
    }

    @Option(name = "-maxcontentsize", usage = "maximum content size", required = false)
    public void setMaxContentSize(int maxContentSize) {
        _maxContentSize = maxContentSize;
    }

    @Option(name = "-commoncrawl", usage = "crawl id for CommonCrawl.org dataset", required = false)
    public void setCommonCrawlId(String commonCrawlId) {
        _commonCrawlId = commonCrawlId;
    }

    @Option(name = "-cachedir", usage = "cache location for CommonCrawl.org secondary index", required = false)
    public void setCommonCrawlCacheDir(String cacheDir) {
        _cacheDir = cacheDir;
    }

    @Option(name = "-fetcherspertask", usage = "fetchers per task", required = false)
    public void setFetchersPerTask(int fetchersPerTask) {
        _fetchersPerTask = fetchersPerTask;
    }

    @Option(name = "-parallelism", usage = "Flink paralellism", required = false)
    public void setParallelism(int parallelism) {
        _parallelism = parallelism;
    }

    @Option(name = "-outputfile", usage = "Local file to store fetched content (testing only)", required = false)
    public void setOutputFile(String outputFile) {
        _outputFile = outputFile;
    }

    @Option(name = "-checkpointdir", usage = "URI to directory to store checkpoint (enables checkpointing)", required = false)
    public void setCheckpointDir(String checkpointDir) {
        _checkpointDir = checkpointDir;
    }

    @Option(name = "-htmlonly", usage = "Only (fully) fetch and parse HTML pages", required = false)
    public void setHtmlOnly(boolean htmlOnly) {
        _htmlOnly = htmlOnly;
    }

    @Option(name = "-nolengthen", usage = "Don't do special processing of shortened URLs", required = false)
    public void setNoLengthen(boolean noLengthen) {
        _noLengthen = noLengthen;
    }

    @Option(name = "-maxoutlinks", usage = "maximum outlinks per page that are extracted", required = false)
    public void setMaxOutlinksPerPage(int maxOutlinksPerPage) {
        _maxOutlinksPerPage = maxOutlinksPerPage;
    }

    public void validate() {
        if (_commonCrawlId == null) {
            if (_userAgent == null) {
                throw new RuntimeException("-agent is required (except for common crawl mode)");
            }
        } else {
            if (_userAgent != null) {
                throw new RuntimeException("user agent not used in common crawl mode");
            }
        }
    }

    public UserAgent getUserAgent() {
        validate();
        return _userAgent;
    }

    public String getSeedUrlsFilename() {
        validate();
        return _urlsFilename;
    }

    public boolean isSingleDomain() {
        validate();
        return (_singleDomain != null);
    }

    public String getSingleDomain() {
        validate();
        return _singleDomain;
    }

    public long getForceCrawlDelay() {
        validate();
        return _forceCrawlDelay;
    }

    public long getDefaultCrawlDelay() {
        validate();
        return _defaultCrawlDelayMS;
    }

    public int getMaxContentSize() {
        validate();
        return _maxContentSize;
    }

    public boolean isCommonCrawl() {
        validate();
        return _commonCrawlId != null;
    }

    public String getCommonCrawlId() {
        validate();
        return _commonCrawlId;
    }

    public String getCommonCrawlCacheDir() {
        validate();
        return _cacheDir;
    }

    public int getFetchersPerTask() {
        validate();
        return _fetchersPerTask;
    }

    public int getParallelism() {
        validate();
        return _parallelism;
    }

    public String getOutputFile() {
        validate();
        return _outputFile;
    }

    public String getCheckpointDir() {
        validate();
        return _checkpointDir;
    }

    public boolean isHtmlOnly() {
        validate();
        return _htmlOnly;
    }

    public boolean isNoLengthen() {
        validate();
        return _noLengthen;
    }

    public int getMaxOutlinksPerPage() {
        return _maxOutlinksPerPage;
    }
}