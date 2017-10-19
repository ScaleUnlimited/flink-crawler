package com.scaleunlimited.flinkcrawler.sources;

import java.io.File;
import java.io.InputStream;
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
 * Source for seed URLs
 * 
 * TODO add checkpointing - see FromElementsFunction.java
 *
 */
@SuppressWarnings("serial")
public class SeedUrlSource extends BaseUrlSource {
	static final Logger LOGGER = LoggerFactory.getLogger(SeedUrlSource.class);

	// For when we're reading from S3
	private String _seedUrlsS3Bucket;
	private String _seedUrlsS3Path;
	private float _estimatedScore;
	private transient InputStream _s3FileStream;
	
	// For when we've read from a local file
	private RawUrl[] _urls;
	
	private volatile boolean _keepRunning = false;
	private transient int _seedUrlIndex;

	/**
	 * Note that we re-order parameters so this doesn't get confused with the constructor that
	 * takes a variable length array of urls.
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
				throw new IllegalArgumentException("Seed urls file doesn't exist in S3: " + seedUrlsFilename);
			}

			_seedUrlsS3Bucket = S3Utils.getBucket(seedUrlsFilename);
			_seedUrlsS3Path = S3Utils.getPath(seedUrlsFilename);
		} else {
			File seedUrlsFile = new File(seedUrlsFilename);
			if (!seedUrlsFile.exists()) {
				throw new IllegalArgumentException("Seed urls file doesn't exist :" + seedUrlsFile.getAbsolutePath());
			}

			List<String> rawUrls = FileUtils.readLines(seedUrlsFile);
			List<RawUrl> seedUrls = new ArrayList<>(rawUrls.size());
			for (String seedUrl : rawUrls) {
				seedUrl = seedUrl.trim();
				if (!seedUrl.isEmpty() && !seedUrl.startsWith("#")) {
					seedUrls.add(new RawUrl(seedUrl, _estimatedScore));
				}
			}
			
			_urls = seedUrls.toArray(new RawUrl[seedUrls.size()]);
		}
	}
	
	public SeedUrlSource(float estimatedScore, String... rawUrls) throws Exception {
		_urls = new RawUrl[rawUrls.length];
		
		for (int i = 0; i < rawUrls.length; i++) {
			String url = rawUrls[i];
			_urls[i] = new RawUrl(url, estimatedScore);
		}
	}

	public SeedUrlSource(RawUrl... rawUrls) {
		_urls = rawUrls;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		_seedUrlIndex = 0;

		if (useS3File()) {
			
//			// TODO move this code into S3Utils? Or S3Helper?
//			// See http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3ClientBuilder.html
//			AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
//			
//			// http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/EnvironmentVariableCredentialsProvider.html
//			builder.setCredentials(new EnvironmentVariableCredentialsProvider());
//			AmazonS3 s3Client = builder.build();
//			S3Object object = s3Client.getObject(new GetObjectRequest(_seedUrlsS3Bucket, _seedUrlsS3Path));
//			_s3FileStream = object.getObjectContent();

			AmazonS3 s3Client = S3Utils.makeS3Client();
			S3Object object = s3Client.getObject(new GetObjectRequest(_seedUrlsS3Bucket, _seedUrlsS3Path));
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
		
		while (_keepRunning) {
			if (useS3File()) {
				// TODO read the next line from the file. Break if we're at the end.
				// Handle skipping empty lines/comments after we've decided if the
				// line is for the appropriate partition, so that we could use the
				// _seedUrlIndex to re-sync if we get terminated/checkpointed/the
				// stream is closed & we have to reopen it, etc.
			} else if (_seedUrlIndex >= _urls.length) {
				break;
			} else {
				if ((_seedUrlIndex % _parallelism) == _index) {
					RawUrl url = _urls[_seedUrlIndex];
					LOGGER.debug(String.format("Emitting %s for partition %d of %d", url, _index, _parallelism));
					context.collect(url);
				}
				
				_seedUrlIndex += 1;
			}
		}
	}
	
	private boolean useS3File() {
		return _seedUrlsS3Bucket != null;
	}

}
