package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
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
import crawlercommons.util.Headers;

/**
 * Implementation of BaseHttpFetcher that uses Common Crawl index files and WARC files
 * to "fetch" documents that were previously fetched by the Common Crawl fetcher and
 * stored in S3.
 * 
 */

@SuppressWarnings("serial")
public class CommonCrawlFetcher extends BaseHttpFetcher {
    private static Logger LOGGER = LoggerFactory.getLogger(CommonCrawlFetcher.class);

    // 0,124,148,146)/index.php 20170429211342 {"url": "http://146.148.124.0/index.php", "mim....
    private static final Pattern CDX_LINE_PATTERN = Pattern.compile("(.+?)[ ]+(\\d+)[ ]+(\\{.+\\})");
    
    private static final Headers EMPTY_HEADERS = new Headers();
    private static final byte[] EMPTY_CONTENT = new byte[0];

    protected static final String DEFAULT_CRAWL_ID = "2017-22";
    protected static final int DEFAULT_THREADS = 1;
	protected static final int DEFAULT_CACHE_SIZE = 8 * 1024 * 1024;
    
    private final String _crawlId;
    private final AmazonS3 _s3Client;
    private final JsonParser _jsonParser;
    private final SegmentCache _cache;
    private final SecondaryIndexMap _secondaryIndexMap;
    
	public CommonCrawlFetcher(AmazonS3 client, String crawlId, int maxThreads, int cacheSize, SecondaryIndexMap secondaryIndexMap) throws IOException {
		// We don't care about the user agent, since we aren't doing real fetches.
		super(maxThreads, new UserAgent("", "", ""));
		
		_s3Client = client;
		_crawlId = crawlId;
		_jsonParser = new JsonParser();
		_cache = new SegmentCache(cacheSize);
		
	    _secondaryIndexMap = secondaryIndexMap;
	}
	
	@Override
	public void abort() {
		// TODO I guess we could try to abort any S3 requests, but they don't
		// typically take very long to complete anyway.
	}

	@Override
	public FetchedResult get(String url, Payload payload) throws BaseFetchException {
        try {
        	LOGGER.debug("Fetch request for " + url);
        	
            URL realUrl = new URL(url);
            String protocol = realUrl.getProtocol();
            if (!protocol.equals("http") && !protocol.equals("https")) {
                throw new BadProtocolFetchException(url);
            }
            
            FetchedResult result = fetch(realUrl, realUrl, payload, 0);
        	LOGGER.debug("Fetched " + url);
            return result;
        } catch (MalformedURLException e) {
            throw new UrlFetchException(url, e.getMessage());
        } catch (AbortedFetchException e) {
            // Don't bother reporting that we bailed because the mime-type
            // wasn't one that we wanted.
            if (e.getAbortReason() != AbortedFetchReason.INVALID_MIMETYPE) {
                LOGGER.debug("Exception fetching {}: {}", url, e.getMessage());
            }
            
            throw e;
        } catch (BaseFetchException e) {
            LOGGER.debug("Exception fetching {}: {}", url, e.getMessage());
            throw e;
		} catch (Exception e) {
            LOGGER.debug("AWS exception loading data for {}: {}", url, e.getMessage());
            throw new UrlFetchException(url, e.getMessage());
		}
	}

	/**
	 * Recursive mthod for attempting to fetch <redirectUrl>.
	 * 
	 * @param originalUrl
	 * @param redirectUrl
	 * @param payload
	 * @param numRedirects
	 * @return
	 * @throws BaseFetchException
	 */
	private FetchedResult fetch(URL originalUrl, URL redirectUrl, Payload payload, int numRedirects) throws BaseFetchException {
		LOGGER.debug(String.format("Fetching '%s' with %d redirects", redirectUrl, numRedirects));
		
		if (numRedirects > getMaxRedirects()) {
			throw new RedirectFetchException(originalUrl.toString(), redirectUrl.toString(), RedirectExceptionReason.TOO_MANY_REDIRECTS);
		}

		// Figure out which segment it's in.
		String targetKey = reverseDomain(redirectUrl);
		SecondaryIndex indexEntry = _secondaryIndexMap.get(targetKey);
		if (indexEntry == null) {
			return make404FetchResult(redirectUrl, numRedirects, payload);
		}
		
		JsonObject jsonObj = findUrlInSegment(redirectUrl, targetKey, indexEntry);
		if (jsonObj == null) {
			return make404FetchResult(redirectUrl, numRedirects, payload);
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
		long timestamp = jsonObj.get("timestamp").getAsLong();

		// We have the data required to fetch the WARC entry and return that as a result.
		GetObjectRequest warcRequest = new GetObjectRequest(S3Utils.getBucket(), warcFile);
		warcRequest.setRange(offset, offset + length);

		String newRedirectUrlAsStr = "";
		try (S3Object warcObject = _s3Client.getObject(warcRequest)) {
			InputStream warcInputStream = new GZIPInputStream(warcObject.getObjectContent());
			DataInputStream dis = new DataInputStream(warcInputStream);
			long startTime = System.currentTimeMillis();
			WarcRecordReader warcReader = new WarcRecordReader(dis);
			WarcRecord pageRecord = warcReader.readNextRecord();
			long deltaTime = Math.max(1L, System.currentTimeMillis() - startTime);
			int bytesRead = warcReader.getBytesRead();
			double responseRateExact = (double)bytesRead / deltaTime;
			// Response rate is bytes/second, not bytes/millisecond
			int responseRate = (int)Math.round(responseRateExact * 1000.0);
			LOGGER.debug(String.format(	"Read %,d bytes from page at %,d offset from '%s' in %,dms (%,d bytes/sec) for '%s'",
										bytesRead,
										offset,
										warcFile,
										deltaTime,
										responseRate,
										redirectUrl));
			Headers headers = new Headers();
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
					LOGGER.warn("Got redirect status but no page record HTTP header == Location");
				}
			}
			
			// if WARC-Truncated is true, and mime-type is not text, then throw an
			// exception (see SimpleHttpFetcher code for example)
			boolean truncated = pageRecord.getHeaderMetadataItem("WARC-Truncated") != null;
			if ((truncated) && (!isTextMimeType(mimeType))) {
				throw new AbortedFetchException(redirectUrl.toString(), "Truncated binary data", AbortedFetchReason.CONTENT_SIZE);
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
						// FUTURE should we check the timestamp to pick the last one?
						result = newResult;
						result.addProperty("timestamp", new Long(Long.parseLong(m.group(2))));
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
	 * Fetch (if needed) and cache the segment data for <indexEntry>.
	 * 
	 * @param url Used for propagating exception if needed.
	 * @param indexEntry Segment entry we need
	 * @return GZipped byte[] of segment data.
	 * @throws IOFetchException
	 */
	private byte[] getSegmentData(URL url, SecondaryIndex indexEntry) throws IOFetchException {
		byte[] result = _cache.get(indexEntry.getSegmentId());
		if (result != null) {
			LOGGER.debug(String.format("Found data for segment #%d in our cache for '%s'", indexEntry.getSegmentId(), url));
			return result;
		}
	
		long length = indexEntry.getSegmentLength();
		result = new byte[(int)length];
		String indexFilename = indexEntry.getIndexFilename();
		String s3Path = S3Utils.makeS3FilePath(_crawlId, indexFilename);
		GetObjectRequest objectRequest = new GetObjectRequest(S3Utils.getBucket(), s3Path);
		long offset = indexEntry.getSegmentOffset();
		objectRequest.setRange(offset, offset + length);
		
		try (S3Object object = _s3Client.getObject(objectRequest)) {
			S3ObjectInputStream is = object.getObjectContent();
			long startTime = System.currentTimeMillis();
			IOUtils.read(is, result);
			long deltaTime = System.currentTimeMillis() - startTime;
			double responseRateExact = (double)length / deltaTime;
			// Response rate is bytes/second, not bytes/millisecond
			int responseRate = (int)Math.round(responseRateExact * 1000.0);
			LOGGER.debug(String.format(	"Read %,d byte segment #%d at %,d offset within %s in %,dms (%,d bytes/sec) for '%s'",
										length,
										indexEntry.getSegmentId(),
										offset,
										indexFilename,
										deltaTime,
										responseRate,
										url));
			
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

	private FetchedResult makeNotOKFetchResult(URL url, String mimeType, int status, int numRedirects, Payload payload) {
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
									numRedirects, 
									url.getHost(), 
									status, 
									"");
	}

	private FetchedResult make404FetchResult(URL url, int numRedirects, Payload payload) {
		return makeNotOKFetchResult(url, "text/plain", HttpStatus.SC_NOT_FOUND, numRedirects, payload);
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

	// TODO Find domain normalization code used in other projects
    protected static String reverseDomain(URL url) {
		StringBuilder reversedUrl = new StringBuilder();

		String domain = url.getHost();
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

		if (url.getPort() != -1) {
			reversedUrl.append(':');
			reversedUrl.append(url.getPort());
		}

		reversedUrl.append(")");

		if (!url.getPath().isEmpty()) {
			reversedUrl.append(url.getPath());
		} else {
			reversedUrl.append('/');
		}

		if (url.getQuery() != null) {
			reversedUrl.append('?');
			reversedUrl.append(url.getQuery());
		}

		return reversedUrl.toString();
    }
    
}
