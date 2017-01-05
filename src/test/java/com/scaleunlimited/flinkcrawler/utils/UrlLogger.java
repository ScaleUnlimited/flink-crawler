package com.scaleunlimited.flinkcrawler.utils;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple3;

import com.scaleunlimited.flinkcrawler.pojos.BaseUrl;

public class UrlLogger {

	private static UrlLogger LOGGER = new UrlLogger();
	
	private static final Map<String, String> EMPTY_MAP = new HashMap<>();
	
	public static void record(Class<?> clazz, BaseUrl url, String... metaData) {
		Map<String, String> metaDataMap = makeMetaDataMap(metaData);
		LOGGER.recordImpl(clazz, url, metaDataMap);
	}

	public static Map<String, String> makeMetaDataMap(String... metaData) {
		if (metaData.length == 0) {
			return EMPTY_MAP;
		}
		
		Map<String, String> metaDataMap = new HashMap<String, String>();
		for (int i = 0; i < metaData.length; i += 2) {
			metaDataMap.put(metaData[i], metaData[i+1]);
		}
		
		return metaDataMap;
	}
	
	public static List<BaseUrl> getByClass(Class<?> clazz) {
		return LOGGER.getByClassImpl(clazz);
	}

	public static List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> getLog() {
		return LOGGER.getLogImpl();
	}
	
	public static UrlLoggerResults getResults() {
		return new UrlLoggerResults(LOGGER.getLogImpl());
	}
	
	// ====================================================================================
	// Private implementation methods
	// ====================================================================================

	private Map<Class<?>, List<BaseUrl>> _byClass;
	private List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> _log;
	
	private UrlLogger() {
		// TODO check system property for whether we're logging, skip otherwise.
		_byClass = new HashMap<>();
		_log = new ArrayList<>();
	}
	
	private void recordImpl(Class<?> clazz, BaseUrl url, Map<String, String> metaData) {
		// TODO use slf4j logging at debug level
		System.out.format("%s: %s\n", clazz.getSimpleName(), url);
		
		List<BaseUrl> urls = _byClass.get(clazz);
		if (urls == null) {
			urls = new ArrayList<BaseUrl>();
			_byClass.put(clazz, urls);
		}
		
		urls.add(url);
		
		_log.add(new Tuple3<Class<?>, BaseUrl, Map<String, String>>(clazz, url, metaData));
	}
	
	private List<BaseUrl> getByClassImpl(Class<?> clazz) {
		return _byClass.get(clazz);
	}
	
	private List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> getLogImpl() {
		return _log;
	}
	
	// ====================================================================================
	// Results that can be used with asserts
	// ====================================================================================

	public static class UrlLoggerResults {

		private List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> _log;
		
		protected UrlLoggerResults(List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> log) {
			_log = log;
		}
		
		/**
		 * Verify we have at least one logging call by <clazz>
		 * 
		 * @param clazz
		 * @return
		 */
		public UrlLoggerResults assertLogging(Class<?> clazz) {
			for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : _log) {
				if (entry.f0.equals(clazz)) {
					return this;
				}
			}
			
			fail("No URLs logged by " + clazz);
			
			// Keep Eclipse happy
			return this;
		}
		
		/**
		 * Verify we have exactly <numCalls> calls logged by <clazz>.
		 * 
		 * @param clazz
		 * @param numCalls
		 * @return
		 */
		public UrlLoggerResults assertLoggedBy(Class<?> clazz, int numCalls) {
			int foundCalls = 0;
			for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : _log) {
				if (entry.f0.equals(clazz)) {
					foundCalls += 1;
				}
			}
			
			if (foundCalls != numCalls) {
				if (foundCalls == 0) {
					fail("No URLs logged by " + clazz);
				} else {
					fail(String.format("Found %d URLs logged by %s, expected %d", foundCalls, clazz, numCalls));
				}
			}
			
			return this;
		}
		
		public UrlLoggerResults assertUrlLoggedBy(Class<?> clazz, BaseUrl url, int numCalls, String... metaData) {
			int foundCalls = 0;
			for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : _log) {
				if (entry.f0.equals(clazz) && entry.f1.equals(url)) {
					Map<String, String> metaDataMap = makeMetaDataMap(metaData);
					for (Map.Entry<String, String> metaDatum : metaDataMap.entrySet()) {
						assertTrue(entry.f2.get(metaDatum.getKey()).equals(metaDatum.getValue()));
					}
					foundCalls += 1;
				}
			}
			
			if (foundCalls != numCalls) {
				if (foundCalls == 0) {
					fail(String.format("URL '%s' not logged by %s", url, clazz));
				} else {
					fail(String.format("URL '%s' was logged %d times by %s, expected %d", url, foundCalls, clazz, numCalls));
				}
			}
			
			return this;
		}
		
		public UrlLoggerResults assertUrlLoggedBy(Class<?> clazz, String url, String... targetMetaData) {
			return assertUrlLoggedBy(clazz, url, 1, targetMetaData);
		}

		public UrlLoggerResults assertUrlLoggedBy(Class<?> clazz, String url, int numCalls, String... targetMetaData) {
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
			
			if (foundCalls != numCalls) {
				if (foundCalls == 0) {
					fail(String.format("URL '%s' not logged by %s", url, clazz));
				} else {
					fail(String.format("URL '%s' was logged %d times by %s, expected %d", url, foundCalls, clazz, numCalls));
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
		

		public UrlLoggerResults assertUrlLogged(BaseUrl url) {
			for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : _log) {
				if (entry.f1.equals(url)) {
					return this;
				}
			}
			
			fail(String.format("Didn't find any logging of URL '%s'", url));
			
			// Keep Eclipse happy
			return this;
		}
		
		public UrlLoggerResults assertUrlNotLoggedBy(Class<?> clazz, BaseUrl url) {
			for (Tuple3<Class<?>, BaseUrl, Map<String, String>> entry : _log) {
				if (entry.f0.equals(clazz) && entry.f1.equals(url)) {
					fail(String.format("Found URL '%s' logged by %s", url, clazz));
				}
			}
			
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
