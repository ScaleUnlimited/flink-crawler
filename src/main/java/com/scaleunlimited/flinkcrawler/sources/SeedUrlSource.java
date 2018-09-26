package com.scaleunlimited.flinkcrawler.sources;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.scaleunlimited.flinkcrawler.config.CrawlTerminator;
import com.scaleunlimited.flinkcrawler.pojos.RawUrl;
import com.scaleunlimited.flinkcrawler.utils.S3Utils;

/**
 * Source for seed URLs.
 * 
 */
@SuppressWarnings("serial")
public class SeedUrlSource extends RichSourceFunction<RawUrl> implements ListCheckpointed<Integer> {
    static final Logger LOGGER = LoggerFactory.getLogger(SeedUrlSource.class);

    // Delay between calls to the collector to emit elemnts.
    private static final long DEFAULT_COLLECTOR_DELAY = 10;

    private CrawlTerminator _terminator;
    private float _estimatedScore;
    private long _collectorDelay = DEFAULT_COLLECTOR_DELAY;
    
    // For when we're reading from S3
    private String _seedUrlsS3Bucket;
    private String _seedUrlsS3Path;
    
    // For when we've read from a local file
    private RawUrl[] _urls;

    private volatile boolean _keepRunning = false;

    private transient InputStream _s3FileStream;
    private transient int _urlIndex;

    /**
     * Note that we re-order parameters so this doesn't get confused with the constructor that takes a variable length
     * array of urls.
     * 
     * @param seedUrlsFilename
     * @param estimatedScore
     * @throws Exception
     */
    public SeedUrlSource(String seedUrlsFilename, float estimatedScore) throws Exception {
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

    public SeedUrlSource(float estimatedScore, String... rawUrls)
            throws Exception {
        _estimatedScore = estimatedScore;

        _urls = new RawUrl[rawUrls.length];

        for (int i = 0; i < rawUrls.length; i++) {
            String url = rawUrls[i];
            _urls[i] = new RawUrl(url, estimatedScore);
        }
    }

    public SeedUrlSource(RawUrl... rawUrls) {
        _urls = rawUrls;
    }

    public CrawlTerminator setTerminator(CrawlTerminator terminator) {
        CrawlTerminator oldTerminator = _terminator;
        _terminator = terminator;
        return oldTerminator;
    }
    
    public long setCollectorDelay(long delay) {
        long oldDelay = _collectorDelay;
        _collectorDelay = delay;
        return oldDelay;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
        int parallelism = context.getNumberOfParallelSubtasks();
        if (parallelism != 1) {
            throw new IllegalStateException("SeedUrlSource only supports a parallelism of 1");
        }

        if (_terminator == null) {
            throw new IllegalStateException("Crawl terminator must be set for the seed URL source");
        }
        
        LOGGER.info("Opening seed URL source");

        // Open the terminator, so that it knows when we really started running.
        _terminator.open();
        
        _urlIndex = 0;
        
        if (useS3File()) {
            AmazonS3 s3Client = S3Utils.makeS3Client();
            S3Object object = s3Client
                    .getObject(new GetObjectRequest(_seedUrlsS3Bucket, _seedUrlsS3Path));
            _s3FileStream = object.getObjectContent();
        }
    }

    @Override
    public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
        List<Integer> state = new ArrayList<>();
        state.add(_urlIndex);
        return state;
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        if (state.size() != 1) {
            throw new IllegalStateException("State must consist of a list of one integer");
        }
        
        _urlIndex = state.get(0);
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("Closing seed URL source");
        
        IOUtils.closeQuietly(_s3FileStream);
        super.close();
    }

    @Override
    public void cancel() {
        LOGGER.info("Cancelling seed URL source");
        
        _keepRunning = false;
    }

    @Override
    public void run(SourceContext<RawUrl> context) throws Exception {
        LOGGER.info("Running seed URL source");

        _keepRunning = true;

        if (useS3File()) {
            runWithS3File(context);
        } else {
            runWithInMemoryList(context);
        }

        LOGGER.info("Terminating seed URL source");
    }

    private void runWithInMemoryList(SourceContext<RawUrl> context) throws IOException {
        while (_keepRunning && !_terminator.isTerminated()) {
            
            if (_urlIndex < _urls.length) {
                RawUrl url = _urls[_urlIndex++];
                LOGGER.debug("Emitting '{}'", url);
                context.collect(url);
            }
            
            try {
                // Sleep so we can be interrupted
                Thread.sleep(_collectorDelay);
            } catch (InterruptedException e) {
                _keepRunning = false;
            }
        }
    }

    private void runWithS3File(SourceContext<RawUrl> context) throws IOException {
        int s3FileLineIndex = 0;
        try (BufferedReader s3FileReader = new BufferedReader(new InputStreamReader(_s3FileStream))) {
            while (_keepRunning && !_terminator.isTerminated()) {
                String sourceLine = s3FileReader.readLine();
                if (sourceLine == null) {
                    break;
                }
                
                if (s3FileLineIndex++ < _urlIndex) {
                    // Skip over previously processed lines if we're restarting 
                    // from checkpointed/saved state. So no delay is needed in
                    // this case.
                    continue;
                }

                String seedUrl = sourceLine.trim();
                if (!seedUrl.isEmpty() && !seedUrl.startsWith("#")) {
                    RawUrl url = new RawUrl(seedUrl, _estimatedScore);
                    LOGGER.debug("Emitting '{}'", url);
                    _urlIndex++;
                    context.collect(url);
                }

                try {
                    // Sleep so we can be interrupted
                    Thread.sleep(_collectorDelay);
                } catch (InterruptedException e) {
                    _keepRunning = false;
                }
            }
        }
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
