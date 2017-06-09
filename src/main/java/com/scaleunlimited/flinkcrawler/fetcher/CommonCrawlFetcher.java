package com.scaleunlimited.flinkcrawler.fetcher;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import crawlercommons.fetcher.AbortedFetchException;
import crawlercommons.fetcher.AbortedFetchReason;
import crawlercommons.fetcher.BadProtocolFetchException;
import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.IOFetchException;
import crawlercommons.fetcher.Payload;
import crawlercommons.fetcher.RedirectFetchException;
import crawlercommons.fetcher.RedirectFetchException.RedirectExceptionReason;
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
    private static final Pattern IDX_LINE_PATTERN = Pattern.compile("(.+?)[ ]+(\\d+)\t(.+?)\t(\\d+)\t(\\d+)\t(\\d+)");
    
    // 0,124,148,146)/index.php 20170429211342 {"url": "http://146.148.124.0/index.php", "mim....
    private static final Pattern CDX_LINE_PATTERN = Pattern.compile("(.+?)[ ]+(\\d+)[ ]+(\\{.+\\})");
    
    private static final Metadata EMPTY_HEADERS = new Metadata();
    private static final byte[] EMPTY_CONTENT = new byte[0];

    private static final String DEFAULT_CRAWL_ID = "2017-17";
	private static final int DEFAULT_THREADS = 1;
	private static final int DEFAULT_CACHE_SIZE = 8 * 1024 * 1024;
    
    private final String _crawlId;
    private final AmazonS3 _s3Client;
    private final String[] _secondaryIndexUrls;
    private final SecondaryIndex[] _secondaryIndex;
    private final JsonParser _jsonParser;
    private final SegmentCache _cache;
    
	public CommonCrawlFetcher() throws IOException {
		this(DEFAULT_CRAWL_ID, DEFAULT_THREADS, DEFAULT_CACHE_SIZE);
	}

	public CommonCrawlFetcher(String crawlId, int maxThreads, int cacheSize) throws IOException {
		super(maxThreads, new UserAgent("", "", ""));
		
		_crawlId = crawlId;
		_jsonParser = new JsonParser();
		_cache = new SegmentCache(cacheSize);
		
		_s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSCredentialsProvider() {
					
                	// TODO do we have to use real credentials? Why didn't anonymous
                	// (null credentials) work?
					@Override
					public void refresh() { }
					
					@Override
					public AWSCredentials getCredentials() {
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
                // TODO control the region???
                .withRegion("us-east-1")
                .build();

		// Fetch the secondary index file, which we need in memory.
		String s3Path = String.format(INDEX_FILES_PATH, _crawlId, SECONDARY_INDEX_FILENAME);
		
		// See if we have the file saved already. If so, read it in.
		File cachedFile = new File("./target/CommonCrawlFetcherData" + s3Path);
		if (!cachedFile.exists()) {
			cachedFile.getParentFile().mkdirs();
			cachedFile.createNewFile();
			FileOutputStream fos = new FileOutputStream(cachedFile);
			try (S3Object object = _s3Client.getObject(new GetObjectRequest(COMMONCRAWL_BUCKET, s3Path))) {
				IOUtils.copy(object.getObjectContent(), fos);
			} finally {
				IOUtils.closeQuietly(fos);
			}
		}
		
		
		try (FileInputStream fis = new FileInputStream(cachedFile)) {
			List<String> lines = IOUtils.readLines(fis);
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
				long offset = Long.parseLong(m.group(4));
				long length = Long.parseLong(m.group(5));
				int id = Integer.parseInt(m.group(6));
				_secondaryIndex[index] = new SecondaryIndex(filename, offset, length, id);
				index++;
			}
		}
	}
	
	@Override
	public void abort() {
		// TODO abort all pending requests.
		// If we have a threaded fetch queue, we can interrupt it, which should work
		// because we can abort an S3 request. But might need to make it async, vs. sync client.
		
	}

	@Override
	public FetchedResult get(String url, Payload payload) throws BaseFetchException {
        try {
            URL realUrl = new URL(url);
            String protocol = realUrl.getProtocol();
            if (!protocol.equals("http") && !protocol.equals("https")) {
                throw new BadProtocolFetchException(url);
            }
            
            // TODO put this into queue, support multi-threading
            // Can we re-use S3Client here?
            return fetch(realUrl, realUrl, payload, 0);
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

	private FetchedResult fetch(URL originalUrl, URL redirectUrl, Payload payload, int numRedirects) throws BaseFetchException {
		if (numRedirects > getMaxRedirects()) {
			throw new RedirectFetchException(originalUrl.toString(), redirectUrl.toString(), RedirectExceptionReason.TOO_MANY_REDIRECTS);
		}

		// TODO move into separate routine, add unit tests
		// TODO Find domain normalization code used in other projects
		
		// First reverse the url
		StringBuilder reversedUrl = new StringBuilder();

		String domain = redirectUrl.getHost();
		String[] domainParts = domain.split("\\.");
		for (int i = domainParts.length - 1; i >= 0; i--) {
			// Skip leading www
 			if ((i > 0) || !domainParts[i].equals("www")) {
				if (reversedUrl.length() > 0) {
					reversedUrl.append(',');
				}
				
				reversedUrl.append(domainParts[i]);
			}
		}

		if (redirectUrl.getPort() != -1) {
			reversedUrl.append(':');
			reversedUrl.append(redirectUrl.getPort());
		}

		reversedUrl.append(")");

		if (!redirectUrl.getPath().isEmpty()) {
			reversedUrl.append(redirectUrl.getPath());
		}

		if (redirectUrl.getQuery() != null) {
			reversedUrl.append('?');
			reversedUrl.append(redirectUrl.getQuery());
		}

		// Now figure out which segment it's in.
		String targetKey = reversedUrl.toString();
		int index = Arrays.binarySearch(_secondaryIndexUrls, targetKey);
		if (index < 0) {
			index = -(index + 1);

			if (index <= 0) {
				return make404FetchResult(redirectUrl, payload);
			}

			// And now we know that the actual index will be the one before the insertion point,
			// so back it off by one.
			index -= 1;
		}

		SecondaryIndex indexEntry = _secondaryIndex[index];
		JsonObject jsonObj = findUrlInSegment(redirectUrl, targetKey, indexEntry);
		if (jsonObj == null) {
			return make404FetchResult(redirectUrl, payload);
		}

		int status = jsonObj.get("status").getAsInt();
		String mimeType = jsonObj.get("mime").getAsString();

		// Decide if we should skip due to mime-type. But we only care about that for
		// the case where we were able to get the page.
		if ((status == HttpStatus.SC_OK) && !isAcceptableMimeType(mimeType)) {
			throw new AbortedFetchException(redirectUrl.toString(), "Invalid mime-type: " + mimeType, AbortedFetchReason.INVALID_MIMETYPE);
		}

		String warcFile = jsonObj.get("filename").getAsString();
		long length = jsonObj.get("length").getAsLong();
		long offset = jsonObj.get("offset").getAsLong();
		long timestamp = System.currentTimeMillis(); // TODO add timestamp to JSON, then jsonObj.get("timestamp").getAsLong();

		// We have the data required to fetch the WARC entry and return that as a result.
		GetObjectRequest warcRequest = new GetObjectRequest(COMMONCRAWL_BUCKET, warcFile);
		warcRequest.setRange(offset, offset + length);

		String newRedirectUrlAsStr = "";
		try (S3Object warcObject = _s3Client.getObject(warcRequest)) {
			InputStream warcInputStream = new GZIPInputStream(warcObject.getObjectContent());
			DataInputStream dis = new DataInputStream(warcInputStream);
			long startTime = System.currentTimeMillis();
			WarcRecordReader warcReader = new WarcRecordReader(dis);
			WarcRecord pageRecord = warcReader.readNextRecord();
			long deltaTime = System.currentTimeMillis() - startTime;
			double responseRateExact = (double)warcReader.getBytesRead() / deltaTime;
			// Response rate is bytes/second, not bytes/millisecond
			int responseRate = (int)Math.round(responseRateExact * 1000.0);

			// if WARC-Truncated is true, and mime-type is not text, then throw an
			// exception (see SimpleHttpFetcher code for example)
			boolean truncated = pageRecord.getHeaderMetadataItem("WARC-Truncated") != null;
			if ((truncated) && (!isTextMimeType(mimeType))) {
				throw new AbortedFetchException(redirectUrl.toString(), "Truncated binary data", AbortedFetchReason.CONTENT_SIZE);
			}

			Metadata headers = new Metadata();
			for (String httpHeader : pageRecord.getHttpHeaders()) {
				String[] keyValue = httpHeader.split(":", 2);
				if (keyValue.length == 2) {
					headers.add(keyValue[0], keyValue[1]);
				} else {
					headers.add(httpHeader, "");
				}
			}
			
			String hostAddress = pageRecord.getHeaderMetadataItem("WARC-IP-Address");
			if (hostAddress == null) {
				hostAddress = redirectUrl.getHost();
			}

			// Handle redirects
			boolean followRedirect = false;
            RedirectExceptionReason reason = null;

            if (getRedirectMode() == RedirectMode.FOLLOW_NONE) {
                switch (status) {
                    case HttpStatus.SC_MOVED_TEMPORARILY:
                    	reason = RedirectExceptionReason.TEMP_REDIRECT_DISALLOWED;
                        break;
                    case HttpStatus.SC_MOVED_PERMANENTLY:
                    	reason = RedirectExceptionReason.PERM_REDIRECT_DISALLOWED;
                        break;
                    case HttpStatus.SC_TEMPORARY_REDIRECT:
                    	reason = RedirectExceptionReason.TEMP_REDIRECT_DISALLOWED;
                        break;
                    case HttpStatus.SC_SEE_OTHER:
                    	reason = RedirectExceptionReason.SEE_OTHER_DISALLOWED;
                        break;
                    default:
                }
            } else if (getRedirectMode() == RedirectMode.FOLLOW_TEMP) {
                switch (status) {
                    case HttpStatus.SC_MOVED_PERMANENTLY:
                    	reason = RedirectExceptionReason.PERM_REDIRECT_DISALLOWED;
                        break;
                    case HttpStatus.SC_SEE_OTHER:
                    	reason = RedirectExceptionReason.SEE_OTHER_DISALLOWED;
                        break;
                	case HttpStatus.SC_MOVED_TEMPORARILY:
                    case HttpStatus.SC_TEMPORARY_REDIRECT:
                    	followRedirect = true;
                    	break;
                    default:
                }
            } else if (getRedirectMode() == RedirectMode.FOLLOW_ALL) {
                switch (status) {
	                case HttpStatus.SC_MOVED_TEMPORARILY:
	                case HttpStatus.SC_MOVED_PERMANENTLY:
	                case HttpStatus.SC_TEMPORARY_REDIRECT:
	                case HttpStatus.SC_SEE_OTHER:
                    	followRedirect = true;
	                    break;
	                default:
                }
            } else {
            	throw new RuntimeException("Unknown redirect mode: " + getRedirectMode());
            }

            if (reason != null) {
                throw new RedirectFetchException(originalUrl.toString(), redirectUrl.toString(), reason);
            }
            
			if (followRedirect) {
				newRedirectUrlAsStr = pageRecord.getHttpHeader("Location");
				if (newRedirectUrlAsStr != null) {
					URL newRedirectUrl = new URL(newRedirectUrlAsStr);
					return fetch(originalUrl, newRedirectUrl, payload, numRedirects + 1);
				} else {
					// TODO log this? We should just keep going and return the data as is.
				}
			}
			
			// FUTURE - use WARC-Target-URI???
			// FUTURE - set up newBaseUrl if we have a "SC_MOVED_PERMANANTLY" result, since we
			// have a new base url.
			FetchedResult pageResult = new FetchedResult(
					originalUrl.toString(),
					redirectUrl.toString(),
					timestamp,
					headers,
					pageRecord.getByteContent(),
					mimeType,
					responseRate,
					payload,
					redirectUrl.toString(),
					numRedirects,
					hostAddress,
					status,
					"");

			return pageResult;
		} catch (MalformedURLException e) {
			throw new UrlFetchException(newRedirectUrlAsStr, e.getMessage());
		} catch (IOException e) {
			throw new IOFetchException(redirectUrl.toString(), e);
		}
	}

	private JsonObject findUrlInSegment(URL url, String targetKey, SecondaryIndex indexEntry) throws IOFetchException {
		byte[] segmentData = getSegmentData(url, indexEntry);
		JsonObject result = null;
		String urlAsStr = url.toString();
		
		try {
			InputStream lis = new GZIPInputStream(new ByteArrayInputStream(segmentData));
			InputStreamReader isr = new InputStreamReader(lis, StandardCharsets.UTF_8);
			BufferedReader lineReader = new BufferedReader(isr);

			String line;
			while ((line = lineReader.readLine()) != null) {
				// 0,124,148,146)/index.php 20170429211342 {"url": "http://146.148.124.0/index.php", "mim....
				Matcher m = CDX_LINE_PATTERN.matcher(line);
				if (!m.matches()) {
					throw new IOException("Invalid CDX line: " + line);
				}

				String entryKey = m.group(1);
				int sort = targetKey.compareTo(entryKey);
				if (sort == 0) {
					// We found a match, now get info from the JSON field.
					String json = m.group(3);
					JsonObject newResult = _jsonParser.parse(json).getAsJsonObject();
					
					// See if the URL in the JsonObject matches what we're looking for.
					String newUrl = newResult.get("url").getAsString();
					if (newUrl.equals(urlAsStr)) {
						// TODO should we check the timestamp to pick the last one?
						result = newResult;
					}
				} else if (sort < 0) {
					// We're past where it could be.
					return result;
				}
			}
		} catch (IOException e) {
			throw new IOFetchException(url.toString(), e);
		}
		
		return result;
	}

	/**
	 * Fetch (if needed) and cache the segment data for <indexEntry>
	 * 
	 * @param url Used for propagating exception if needed.
	 * @param indexEntry Segment entry we need
	 * @return GZipped byte[] of segment data.
	 * @throws IOFetchException
	 */
	private byte[] getSegmentData(URL url, SecondaryIndex indexEntry) throws IOFetchException {
		byte[] result = _cache.get(indexEntry.getSegmentId());
		if (result != null) {
			return result;
		}
	
		result = new byte[(int)indexEntry.getSegmentLength()];
		String s3Path = String.format(INDEX_FILES_PATH, "2017-17", indexEntry.getIndexFilename());
		GetObjectRequest objectRequest = new GetObjectRequest(COMMONCRAWL_BUCKET, s3Path);
		objectRequest.setRange(indexEntry.getSegmentOffset(), indexEntry.getSegmentOffset() + indexEntry.getSegmentLength());
		
		try (S3Object object = _s3Client.getObject(objectRequest)) {
			S3ObjectInputStream is = object.getObjectContent();
			IOUtils.read(is, result);
			
			_cache.put(indexEntry.getSegmentId(), result);
			return result;
		} catch (IOException e) {
			throw new IOFetchException(url.toString(), e);
		}
	}

	/**
	 * Return true if <mimeType> is acceptable. This is always true
	 * if the fetcher hasn't been configured to filter by mime-type.
	 * 
	 * @param mimeType
	 * @return
	 */
	private boolean isAcceptableMimeType(String mimeType) {
        Set<String> mimeTypes = getValidMimeTypes();
        if ((mimeTypes != null) && (mimeTypes.size() > 0)) {
            return mimeTypes.contains(mimeType);
        } else {
        	return true;
        }
	}

	private FetchedResult makeNotOKFetchResult(URL url, String mimeType, int status, Payload payload) {
		String urlAsString = url.toString();
		return new FetchedResult(	urlAsString,
									urlAsString, 
									System.currentTimeMillis(), 
									EMPTY_HEADERS, 
									EMPTY_CONTENT, 
									mimeType, 
									0, 
									payload, 
									urlAsString, 
									0, 
									url.getHost(), 
									status, 
									"");
	}

	private FetchedResult make404FetchResult(URL url, Payload payload) {
		return makeNotOKFetchResult(url, "text/plain", HttpStatus.SC_NOT_FOUND, payload);
	}

	// FUTURE is there a better way to figure out what a good set of mime-types is?
	// And can we use text/* as a more general option?
    private static final String TEXT_MIME_TYPES[] = {
    		"text/plain", 
    		"text/html", 
    		"application/x-asp", 
    		"application/xhtml+xml", 
    		"application/vnd.wap.xhtml+xml"
    };

    private boolean isTextMimeType(String mimeType) {
        for (String textContentType : TEXT_MIME_TYPES) {
            if (textContentType.equals(mimeType)) {
                return true;
            }
        }
        
        return false;
    }

	private static class SecondaryIndex {
		private String _indexFilename;
		
		private long _segmentOffset;
		private long _segmentLength;
		private int _segmentId;
		
		public SecondaryIndex(String indexFilename, long segmentOffset, long segmentLength, int segmentId) {
			_indexFilename = indexFilename;
			_segmentOffset = segmentOffset;
			_segmentLength = segmentLength;
			_segmentId = segmentId;
		}

		public String getIndexFilename() {
			return _indexFilename;
		}

		public long getSegmentOffset() {
			return _segmentOffset;
		}

		public long getSegmentLength() {
			return _segmentLength;
		}

		public int getSegmentId() {
			return _segmentId;
		}
	}
	
	private static class SegmentCache {
		private static final int AVERAGE_SEGMENT_SIZE = 190_000;
		
		private LinkedHashMap<Integer, byte[]> _cache;
		private final int _maxCacheSize;
		
		public SegmentCache(int maxCacheSize) {
			_maxCacheSize = maxCacheSize;
			int targetEntries = Math.max(1, maxCacheSize / AVERAGE_SEGMENT_SIZE);
			_cache = new LinkedHashMap<Integer, byte[]>(targetEntries, 0.75f, true) {
				
				@Override
				protected boolean removeEldestEntry(java.util.Map.Entry<Integer, byte[]> eldest) {
					return calcCacheSize() > _maxCacheSize;
				}
				
				private int calcCacheSize() {
					int curSize = 0;
					for (byte[] data : values()) {
						curSize += data.length;
					}
					
					return curSize;
				}
			};
		}

		public void put(int segmentId, byte[] data) {
			_cache.put(segmentId, data);
		}

		public byte[] get(int segmentId) {
			return _cache.get(segmentId);
		}
		
		
	}
}
