package com.scaleunlimited.flinkcrawler.utils;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.BaseUrl;

public class UrlLoggerImpl implements IUrlLogger {
	static final Logger LOGGER = LoggerFactory.getLogger(UrlLoggerImpl.class);
	
	private static final Map<String, String> EMPTY_METADATA_MAP = new HashMap<>();
	
	private Map<Class<?>, List<BaseUrl>> _byClass;
	private List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> _log;
	
	public UrlLoggerImpl() {
		// TODO check system property for whether we're logging, skip otherwise.
		_byClass = new HashMap<>();
		_log = new ArrayList<>();
	}
	
	public void record(Class<?> clazz, BaseUrl url, String... metaData) {
		record(clazz, url, makeMetaDataMap(metaData));
	}
	
	public void record(Class<?> clazz, BaseUrl url, Map<String, String> metaData) {
		LOGGER.debug(String.format("%s: %s", clazz.getSimpleName(), url));
		
		List<BaseUrl> urls = _byClass.get(clazz);
		if (urls == null) {
			urls = new ArrayList<BaseUrl>();
			_byClass.put(clazz, urls);
		}
		
		urls.add(url);
		
		_log.add(new Tuple3<Class<?>, BaseUrl, Map<String, String>>(clazz, url, metaData));
	}
	
	public List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> getLog() {
		return _log;
	}
	
	private static Map<String, String> makeMetaDataMap(String... metaData) {
		if (metaData.length == 0) {
			return EMPTY_METADATA_MAP;
		}
		
		Map<String, String> metaDataMap = new HashMap<String, String>();
		for (int i = 0; i < metaData.length; i += 2) {
			metaDataMap.put(metaData[i], metaData[i+1]);
		}
		
		return metaDataMap;
	}
	
	
	// ====================================================================================
	// Results that can be used with asserts
	// ====================================================================================

	public static class UrlLoggerResults {

		private List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> _log;
		
		public UrlLoggerResults(List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> log) {
			_log = log;
		}
		
		/**
		 * Verify we have at least one logging call by <clazz>
		 * 
		 * @param clazz
		 * @return
		 */
		public UrlLoggerResults assertLogging(Class<?> clazz) {
			return assertLoggedBy(clazz, 1, Integer.MAX_VALUE);
		}
		
		/**
		 * Verify we have exactly <numCalls> calls logged by <clazz>.
		 * 
		 * @param clazz
		 * @param minCalls
		 * @return
		 */
		public UrlLoggerResults assertLoggedBy(Class<?> clazz, int numCalls) {
			return assertLoggedBy(clazz, numCalls, numCalls);
		}
		
		/**
		 * Verify we have between <minCalls> and <maxCalls> calls logged by <clazz>.
		 * 
		 * @param clazz
		 * @param minCalls
		 * @param maxCalls
		 * @return
		 */
		public UrlLoggerResults assertLoggedBy(Class<?> clazz, int minCalls, int maxCalls) {
			int foundCalls = 0;
			for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : _log) {
				if (entry.f0.equals(clazz)) {
					foundCalls += 1;
				}
			}
			
			if ((foundCalls < minCalls) || (foundCalls > maxCalls)) {
				if (foundCalls == 0) {
					fail("No URLs logged by " + clazz);
				} else if (minCalls == maxCalls) {
					fail(String.format("Found %d URLs logged by %s, expected %d", foundCalls, clazz, minCalls));
				} else {
					fail(String.format("Found %d URLs logged by %s, expected between %d and %d", foundCalls, clazz, minCalls, maxCalls));
				}
			}
			
			return this;
		}
		
		public UrlLoggerResults assertUrlLoggedBy(Class<?> clazz, String url, String... targetMetaData) {
			return assertUrlLoggedBy(clazz, url, 1, Integer.MAX_VALUE, targetMetaData);
		}

		public UrlLoggerResults assertUrlLoggedBy(Class<?> clazz, String url, int minCalls, String... targetMetaData) {
			return assertUrlLoggedBy(clazz, url, minCalls, minCalls, targetMetaData);
		}

		public UrlLoggerResults assertUrlLoggedBy(Class<?> clazz, String url, int minCalls, int maxCalls, String... targetMetaData) {
			Map<String, String> targetMetaDataMap = makeMetaDataMap(targetMetaData);

			int foundCalls = 0;
			for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : _log) {
				if (entry.f0.equals(clazz) && entry.f1.getUrl().equals(url)) {
					// For every entry that we care about in the target metadata, make sure it exists
					// and has the same value.
					boolean metaDataMatches = true;
					for (String targetMetaDataKey : targetMetaDataMap.keySet()) {
						if (!entry.f2.containsKey(targetMetaDataKey)) {
							metaDataMatches = false;
							break;
						} else if (!entry.f2.get(targetMetaDataKey).equals(targetMetaDataMap.get(targetMetaDataKey))) {
							metaDataMatches = false;
							break;
						}
 					}
					
					if (metaDataMatches) {
						foundCalls += 1;
					}
				}
			}
			
			if ((foundCalls < minCalls) || (foundCalls > maxCalls)) {
				if (foundCalls == 0) {
					fail(String.format("URL '%s' not logged by %s", url, clazz));
				} else if (minCalls == maxCalls) {
					fail(String.format("URL '%s' was logged %d times by %s, expected %d", url, foundCalls, clazz, minCalls));
				} else {
					fail(String.format("URL '%s' was logged %d times by %s, expected between %d and %d", foundCalls, clazz, minCalls, maxCalls));
				}
			}
			
			return this;
		}
		
		/**
		 * Verify we have no URLs logged by <clazz>.
		 * 
		 * @param clazz
		 * @return
		 */
		public UrlLoggerResults assertNotCalledBy(Class<?> clazz) {
			for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : _log) {
				if (entry.f0.equals(clazz)) {
					fail(String.format("Found URL '%s' logged by %s", entry.f1, clazz));
				}
			}
			
			return this;
		}
		

		public UrlLoggerResults assertUrlLogged(String url) {
			for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : _log) {
				if (entry.f1.getUrl().equals(url)) {
					return this;
				}
			}
			
			fail(String.format("Didn't find any logging of URL '%s'", url));
			
			// Keep Eclipse happy
			return this;
		}
		
		public UrlLoggerResults assertUrlNotLoggedBy(Class<?> clazz, String url) {
			for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : _log) {
				if (entry.f0.equals(clazz) && entry.f1.getUrl().equals(url)) {
					fail(String.format("Found URL '%s' logged by %s", url, clazz));
				}
			}
			
			return this;
		}
		

	}
	
}
