package com.scaleunlimited.flinkcrawler.sources;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.utils.S3Utils;

/**
 * Source for seed URLs.
 * 
 * Generate special CrawlStateUrl values (ticklers) that keep the iteration
 * running, until we're stopped. When that happens, generate termination
 * CrawStateUrls that tell the CrawlDB to flush the fetch queue and not
 * fill it any longer.
 * 
 * TODO add checkpointing - see FromElementsFunction.java
 *
 */
@SuppressWarnings("serial")
public class SeedUrlSource extends BaseUrlSource {
    static final Logger LOGGER = LoggerFactory.getLogger(SeedUrlSource.class);

    // Time between tickler URLs.
    private static final long TICKLER_INTERVAL = 100L;

    // For when we're reading from S3
    private String _seedUrlsS3Bucket;
    private String _seedUrlsS3Path;
    private float _estimatedScore;
    private int _crawlDbParallelism;
    
    // For when we've read from a local file
    private RawUrl[] _urls;

    private transient InputStream _s3FileStream;
    private volatile boolean _keepRunning = false;
    private transient int _seedUrlIndex;
    private transient RawUrl[] _ticklers;
    
    /**
     * Note that we re-order parameters so this doesn't get confused with the constructor that takes a variable length
     * array of urls.
     * 
     * @param seedUrlsFilename
     * @param estimatedScore
     * @throws Exception
     */
    public SeedUrlSource(int crawlDbParallelism, String seedUrlsFilename, float estimatedScore) throws Exception {
        _crawlDbParallelism = crawlDbParallelism;
        _estimatedScore = estimatedScore;
        
        // If it's an S3 file, we delay processing until we're running, as the file could be really
        // big so we want to incrementally consume it.
        if (S3Utils.isS3File(seedUrlsFilename)) {
            if (!S3Utils.fileExists(seedUrlsFilename)) {
                throw new IllegalArgumentException(
                        "Seed urls file doesn't exist in S3: " + seedUrlsFilename);
            }

            _seedUrlsS3Bucket = S3Utils.getBucket(seedUrlsFilename);
            _seedUrlsS3Path = S3Utils.getPath(seedUrlsFilename);
        } else {
            File seedUrlsFile = new File(seedUrlsFilename);
            if (!seedUrlsFile.exists()) {
                throw new IllegalArgumentException(
                        "Seed urls file doesn't exist :" + seedUrlsFile.getAbsolutePath());
            }

            List<String> rawUrls = FileUtils.readLines(seedUrlsFile);
            List<RawUrl> seedUrls = new ArrayList<>(rawUrls.size());
            for (String seedUrl : rawUrls) {
                RawUrl parsedUrl = parseSourceLine(seedUrl);
                if (parsedUrl != null) {
                    seedUrls.add(parsedUrl);
                }
            }

            _urls = seedUrls.toArray(new RawUrl[seedUrls.size()]);
        }
    }

    public SeedUrlSource(int crawlDbParallelism, float estimatedScore, String... rawUrls) throws Exception {
        _crawlDbParallelism = crawlDbParallelism;
        _estimatedScore = estimatedScore;

        _urls = new RawUrl[rawUrls.length];

        for (int i = 0; i < rawUrls.length; i++) {
            String url = rawUrls[i];
            _urls[i] = new RawUrl(url, estimatedScore);
        }
    }

    public SeedUrlSource(int crawlDbParallelism, RawUrl... rawUrls) {
        _crawlDbParallelism = crawlDbParallelism;

        _urls = rawUrls;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        _seedUrlIndex = 0;

        _ticklers = new RawUrl[_crawlDbParallelism];
        for (int i = 0; i < _crawlDbParallelism; i++) {
            _ticklers[i] = RawUrl.makeRawTickerUrl(getMaxParallelism(), _crawlDbParallelism, i);
        }
        
        if (useS3File()) {
            AmazonS3 s3Client = S3Utils.makeS3Client();
            S3Object object = s3Client
                    .getObject(new GetObjectRequest(_seedUrlsS3Bucket, _seedUrlsS3Path));
            _s3FileStream = object.getObjectContent();
        }
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(_s3FileStream);
        super.close();
    }

    @Override
    public void cancel() {
        _keepRunning = false;
    }

    @Override
    public void run(SourceContext<RawUrl> context) throws Exception {
        _keepRunning = true;

        BufferedReader s3FileReader = null;
        if (useS3File()) {
            s3FileReader = new BufferedReader(new InputStreamReader(_s3FileStream));
        }

        long nextTickleTime = 0;
        while (_keepRunning) {

            // TODO Handle skipping empty lines/comments after we've decided if
            // the line is for the appropriate partition, so that we could use
            // _seedUrlIndex to re-sync if we get terminated/checkpointed/the
            // stream is closed & we have to reopen it, etc.
            //
            // Hmmm. That would make _seedUrlIndex an input line index rather
            // than a URL index, no? Assuming the file contents don't change,
            // it seems like we could either save a separate input line index
            // (i.e., to save re-parsing each input line for the re-sync),
            // or we just let it spin through the file re-parsing each line
            // until we've seen _seedUrlIndex real URLs.
            //
            if (useS3File()) {
                String sourceLine = s3FileReader.readLine();
                if (sourceLine == null) {
                    break;
                }
                RawUrl url = parseSourceLine(sourceLine);
                if (url != null) {
                    collectUrl(url, context);
                }
            } else if (_seedUrlIndex < _urls.length) {
                collectUrl(_urls[_seedUrlIndex++], context);
            }
            
            // Now see if it's time to emit tickler URLs.
            long curTime = System.currentTimeMillis();
            if (curTime >= nextTickleTime) {
                for (RawUrl tickler : _ticklers) {
                    collectUrl(tickler, context);
                }
                
                nextTickleTime = curTime + TICKLER_INTERVAL;
            }
            
            try {
                // Sleep so we can be interrupted
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                _keepRunning = false;
            }
        }

        if (s3FileReader != null) {
            s3FileReader.close();
        }
        
        // TODO(kkrugler) - generate termination URLs, then wait for the configured
        // amount of time before we actually return, and thus really end.
    }

    private void collectUrl(RawUrl url, SourceContext<RawUrl> context) {
        if (url.isRegular()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("Emitting %s", url));
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Emitting %s", url));
            }
        }
        
        context.collect(url);
    }

    private boolean useS3File() {
        return _seedUrlsS3Bucket != null;
    }

    private RawUrl parseSourceLine(String sourceLine) throws Exception {
        String seedUrl = sourceLine.trim();
        if (seedUrl.isEmpty() || seedUrl.startsWith("#")) {
            return null;
        }
        return new RawUrl(seedUrl, _estimatedScore);
    }

}
