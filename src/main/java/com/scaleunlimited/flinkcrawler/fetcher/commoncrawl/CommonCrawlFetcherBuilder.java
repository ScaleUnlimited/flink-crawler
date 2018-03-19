package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.scaleunlimited.flinkcrawler.fetcher.BaseHttpFetcherBuilder;

import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

@SuppressWarnings("serial")
public class CommonCrawlFetcherBuilder extends BaseHttpFetcherBuilder {
    private static Logger LOGGER = LoggerFactory.getLogger(CommonCrawlFetcherBuilder.class);

    private static final String SERIALIZED_SECONDARY_INDEX_FILENAME = "secondary_index.bin.gz";
    private static final String SECONDARY_INDEX_FILENAME = "cluster.idx";

    private String _crawlId;
    private File _cachedFile;

    public CommonCrawlFetcherBuilder(int maxSimultaneousRequests, UserAgent userAgent,
            String crawlId, String cacheDir) {
        super(maxSimultaneousRequests, userAgent);
        _crawlId = crawlId;
        if (cacheDir != null) {
            _cachedFile = makeCacheFile(new File(cacheDir));
            try {
                prepCache(_cachedFile);
            } catch (IOException e) {
                throw new RuntimeException("Unable to prepare secondary index file cache", e);
            }
        }
    }

    private void prepCache(File cachedFile) throws IOException {
        // Load the cache with the serialized secondary index file.

        AmazonS3 client = makeClient();
        if (!cachedFile.exists()) {
            // Fetch the secondary index file, which we need in memory.
            String s3Path = S3Utils.makeS3FilePath(_crawlId, SECONDARY_INDEX_FILENAME);

            LOGGER.info("Downloading and parsing secondary index file for " + _crawlId + " from "
                    + s3Path);

            try (S3Object object = client
                    .getObject(new GetObjectRequest(S3Utils.getBucket(), s3Path))) {
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(object.getObjectContent(), StandardCharsets.UTF_8));
                SecondaryIndexMap.Builder builder = new SecondaryIndexMap.Builder(1_100_000);
                String line;
                while ((line = br.readLine()) != null) {
                    builder.add(line);
                }

                SecondaryIndexMap secondaryIndexMap = builder.build();

                // Serialize the map for next time.
                cachedFile.getParentFile().mkdirs();
                cachedFile.createNewFile();
                LOGGER.info("Saving serialized secondary index file for " + _crawlId + " to "
                        + cachedFile);
                try (DataOutputStream out = new DataOutputStream(
                        new GZIPOutputStream(new FileOutputStream(cachedFile)))) {
                    secondaryIndexMap.write(out);
                }
            }
        }
    }

    private AmazonS3 makeClient() {
        return AmazonS3ClientBuilder.standard().withCredentials(new MyS3CredentialsProviderChain())
                // TODO control the region???
                .withRegion("us-east-1").build();
    }

    private File makeCacheFile(File cacheDir) {
        if (_crawlId == null) {
            throw new IllegalStateException("Can't set up cache file if crawl id hasn't been set");
        }
        return new File(cacheDir,
                String.format("%s-%s", _crawlId, SERIALIZED_SECONDARY_INDEX_FILENAME));
    }

    @Override
    public BaseHttpFetcher build() throws Exception {
        AmazonS3 client = makeClient();

        // If the caller hasn't set up a cachedFile then we create one in a temp location
        if (_cachedFile == null) {
            File tempDir = File.createTempFile("cache-dir", "");
            tempDir.delete();
            tempDir.mkdir();
            _cachedFile = makeCacheFile(tempDir);
            prepCache(_cachedFile);
        }

        LOGGER.info("Loading serialized secondary index for cache from " + _cachedFile);
        try (DataInputStream in = new DataInputStream(
                new GZIPInputStream(new FileInputStream(_cachedFile)))) {
            SecondaryIndexMap secondaryIndexMap = new SecondaryIndexMap();
            secondaryIndexMap.read(in);
            CommonCrawlFetcher result = new CommonCrawlFetcher(client, _crawlId,
                    _maxSimultaneousRequests, CommonCrawlFetcher.DEFAULT_CACHE_SIZE,
                    secondaryIndexMap);
            return configure(result);
        }
    }

    /**
     * We want to use the S3CredentialsProviderChain, which supports the --no-sign-request (CLI) option, but that's not
     * visible. So always pretend like we have no credentials.
     *
     */
    private static class MyS3CredentialsProviderChain extends DefaultAWSCredentialsProviderChain {

        @Override
        public AWSCredentials getCredentials() {
            return null;
        }
    }

}
