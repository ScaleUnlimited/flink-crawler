package com.scaleunlimited.flinkcrawler.fetcher;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import crawlercommons.fetcher.AbortedFetchException;
import crawlercommons.fetcher.AbortedFetchReason;
import crawlercommons.fetcher.BadProtocolFetchException;
import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.IOFetchException;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.UrlFetchException;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;

@SuppressWarnings("serial")
public class CommonCrawlFetcher extends BaseHttpFetcher {
    private static Logger LOGGER = LoggerFactory.getLogger(CommonCrawlFetcher.class);

    // Path to files in S3 looks like s3://commoncrawl/cc-index/collections/CC-MAIN-2017-17/indexes/cluster.idx
    // "2017-17" is a crawl ID
    // "cluster.idx" is the name of an index file.
    
    private static final String COMMONCRAWL_BUCKET = "commoncrawl";
    private static final String INDEX_FILES_PATH = "cc-index/collections/CC-MAIN-%s/indexes/%s";    
    private static final String SECONDARY_INDEX_FILENAME = "cluster.idx";
    
    // Format is <reversed domain>)<path><space><timestamp>\t<filename>\t<offset>\t<length>\t<sequence id>
    // 146,207,118,124)/interviewpicservlet?curpage=0&sort=picall 20170427202839       cdx-00000.gz    4750379 186507  26
    private static final Pattern IDX_LINE_PATTERN = Pattern.compile("(.+)[ ]+(\\d+)\t(.+)\t(\\d+)\t(\\d+)\t(\\d+)");
    
    // 0,124,148,146)/index.php 20170429211342 {"url": "http://146.148.124.0/index.php", "mim....
    private static final Pattern CDX_LINE_PATTERN = Pattern.compile("(.+)[ ]+(\\d+)[ ]+(\\{.+\\})");
    
    private static final Metadata EMPTY_HEADERS = new Metadata();
    private static final byte[] EMPTY_CONTENT = new byte[0];
    
    
    
    private final AmazonS3 _s3Client;
    private final String[] _secondaryIndexUrls;
    private final SecondaryIndex[] _secondaryIndex;
    
	public CommonCrawlFetcher() throws IOException {
		this(1);
	}

	public CommonCrawlFetcher(int maxThreads) throws IOException {
		super(maxThreads, new UserAgent("", "", ""));
		
		_s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSCredentialsProvider() {
					
					@Override
					public void refresh() { }
					
					@Override
					public AWSCredentials getCredentials() {
						// TODO Auto-generated method stub
						return new AWSCredentials() {
							
							@Override
							public String getAWSSecretKey() {
								return "WinQ+Z/9lQlaTtGjS/DkDKe3DEH9Uhqgbho/Hzuv";
							}
							
							@Override
							public String getAWSAccessKeyId() {
								return "AKIAISPKLQBUFS4XBDKQ";
							}
						};
					}
				})
                .withRegion("us-east-1")
                .build();

		// Fetch the secondary index file, which we need in memory.
		// TODO get crawl ID from configuration, or passed in.
		String s3Path = String.format(INDEX_FILES_PATH, "2017-17", SECONDARY_INDEX_FILENAME);
		
		try (S3Object object = _s3Client.getObject(new GetObjectRequest(COMMONCRAWL_BUCKET, s3Path))) {
			List<String> lines = IOUtils.readLines(object.getObjectContent());
			int numEntries = lines.size();
			_secondaryIndexUrls = new String[numEntries];
			_secondaryIndex = new SecondaryIndex[numEntries];
			
			int index = 0;
			for (String line : lines) {
				// 0,124,148,146)/index.php 20170429211342 cdx-00000.gz 0 195191 1
				Matcher m = IDX_LINE_PATTERN.matcher(line);
				if (!m.matches()) {
					throw new IOException("Invalid .idx line: " + line);
				}
				_secondaryIndexUrls[index] = m.group(1);
				
				String filename = m.group(3);
				int offset = Integer.parseInt(m.group(4));
				int length = Integer.parseInt(m.group(5));
				int id = Integer.parseInt(m.group(6));
				
				_secondaryIndex[index] = new SecondaryIndex(filename, offset, length, id);
				index++;
			}
		}
	}
	
	@Override
	public void abort() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public FetchedResult get(String url, Payload payload) throws BaseFetchException {
        try {
            URL realUrl = new URL(url);
            String protocol = realUrl.getProtocol();
            if (!protocol.equals("http") && !protocol.equals("https")) {
                throw new BadProtocolFetchException(url);
            }
            
            return fetch(realUrl, payload);
        } catch (MalformedURLException e) {
            throw new UrlFetchException(url, e.getMessage());
        } catch (AbortedFetchException e) {
            // Don't bother reporting that we bailed because the mime-type
            // wasn't one that we wanted.
            if (e.getAbortReason() != AbortedFetchReason.INVALID_MIMETYPE) {
                LOGGER.debug("Exception fetching {} {}", url, e.getMessage());
            }
            
            throw e;
        } catch (BaseFetchException e) {
            LOGGER.debug("Exception fetching {} {}", url, e.getMessage());
            throw e;
        }
	}

	private FetchedResult fetch(URL url, Payload payload) throws AbortedFetchException, IOFetchException {
		// First reverse the url
		StringBuilder reversedUrl = new StringBuilder();
		
		String domain = url.getHost();
		String[] domainParts = domain.split("\\.");
		for (int i = domainParts.length - 1; i >= 0; i--) {
			reversedUrl.append(domainParts[i]);
			if (i > 0) {
				reversedUrl.append(',');
			}
		}
		
		if (url.getPort() != -1) {
			reversedUrl.append(':');
			reversedUrl.append(url.getPort());
		}
		
		reversedUrl.append(")");
		
		if (!url.getPath().isEmpty()) {
			reversedUrl.append(url.getPath());
		}
		
		if (url.getQuery() != null) {
			reversedUrl.append('?');
			reversedUrl.append(url.getQuery());
		}
		
		// Now figure out which segment it's in.
		String targetKey = reversedUrl.toString();
		int index = Arrays.binarySearch(_secondaryIndexUrls, targetKey);
		if (index < 0) {
			index = -(index + 1);
			
			if (index <= 0) {
				return make404FetchResult(url, payload);
			}
			
			// And now we know that the actual index will be the one before the insertion point,
			// so back it off by one.
			index -= 1;
		}
		
		SecondaryIndex indexEntry = _secondaryIndex[index]; 
		String s3Path = String.format(INDEX_FILES_PATH, "2017-17", indexEntry.getIndexFilename());
		GetObjectRequest objectRequest = new GetObjectRequest(COMMONCRAWL_BUCKET, s3Path);
		objectRequest.setRange(indexEntry.getSegmentOffset(), indexEntry.getSegmentOffset() + indexEntry.getSegmentLength());
		
		try (S3Object object = _s3Client.getObject(objectRequest)) {
			S3ObjectInputStream is = object.getObjectContent();
			InputStream lis = new GZIPInputStream(is);
			InputStreamReader isr = new InputStreamReader(lis, StandardCharsets.UTF_8);
			BufferedReader lineReader = new BufferedReader(isr);
			
			String line;
			while ((line = lineReader.readLine()) != null) {
				// 0,124,148,146)/index.php 20170429211342 {"url": "http://146.148.124.0/index.php", "mim....
				Matcher m = CDX_LINE_PATTERN.matcher(line);
				if (!m.matches()) {
					throw new IOFetchException(url.toString(), new IOException("Invalid CDX line: " + line));
				}
				
				String entryKey = m.group(1);
				int sort = targetKey.compareTo(entryKey);
				if (sort == 0) {
					// we found it
					// TODO get info from JSON, fetch page from WARC file, create FetchResult.
					String json = m.group(3);
					
					return null;
				} else if (sort < 0) {
					// we're past where it could be.
					return make404FetchResult(url, payload);
				}
			}
			
			// Didn't find it.
			return make404FetchResult(url, payload);
		} catch (IOException e) {
			throw new IOFetchException(url.toString(), e);
		}
	}

	private FetchedResult make404FetchResult(URL url, Payload payload) {
		String urlAsString = url.toString();
		// TODO Auto-generated method stub
		return new FetchedResult(	urlAsString,
									urlAsString, 
									System.currentTimeMillis(), 
									EMPTY_HEADERS, 
									EMPTY_CONTENT, 
									"text/plain", 
									0, 
									payload, 
									urlAsString, 
									0, 
									url.getHost(), 
									HttpStatus.SC_GONE, 
									"");
	}

	private static class SecondaryIndex {
		private String _indexFilename;
		
		private int _segmentOffset;
		private int _segmentLength;
		private int _segmentId;
		
		public SecondaryIndex(String indexFilename, int segmentOffset, int segmentLength, int segmentId) {
			_indexFilename = indexFilename;
			_segmentOffset = segmentOffset;
			_segmentLength = segmentLength;
			_segmentId = segmentId;
		}

		public String getIndexFilename() {
			return _indexFilename;
		}

		public int getSegmentOffset() {
			return _segmentOffset;
		}

		public int getSegmentLength() {
			return _segmentLength;
		}

		public int getSegmentId() {
			return _segmentId;
		}
		
	}
}
