package com.scaleunlimited.flinkcrawler.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Assert;

import com.scaleunlimited.flinkcrawler.pojos.BaseUrl;

public class UrlLogger {

	private static UrlLogger LOGGER = new UrlLogger();
	
	public static void record(Class<?> clazz, BaseUrl url) {
		LOGGER.recordImpl(clazz, url);
	}
	
	public static List<BaseUrl> getByClass(Class<?> clazz) {
		return LOGGER.getByClassImpl(clazz);
	}

	public static List<Tuple2<Class<?>, BaseUrl>> getLog() {
		return LOGGER.getLogImpl();
	}
	
	public static UrlLoggerResults getResults() {
		return new UrlLoggerResults(LOGGER.getLogImpl());
	}
	
	// ====================================================================================
	// Private implementation methods
	// ====================================================================================

	private Map<Class<?>, List<BaseUrl>> _byClass;
	private List<Tuple2<Class<?>, BaseUrl>> _log;
	
	private UrlLogger() {
		// TODO check system property for whether we're logging, skip otherwise.
		_byClass = new HashMap<>();
		_log = new ArrayList<>();
	}
	
	private void recordImpl(Class<?> clazz, BaseUrl url) {
		// TODO use slf4j logging at debug level
		System.out.format("%s: %s\n", clazz.getSimpleName(), url);
		
		List<BaseUrl> urls = _byClass.get(clazz);
		if (urls == null) {
			urls = new ArrayList<BaseUrl>();
			_byClass.put(clazz, urls);
		}
		
		urls.add(url);
		
		_log.add(new Tuple2<Class<?>, BaseUrl>(clazz, url));
	}
	
	private List<BaseUrl> getByClassImpl(Class<?> clazz) {
		return _byClass.get(clazz);
	}
	
	private List<Tuple2<Class<?>, BaseUrl>> getLogImpl() {
		return _log;
	}
	
	// ====================================================================================
	// Results that can be used with asserts
	// ====================================================================================

	public static class UrlLoggerResults {

		private List<Tuple2<Class<?>, BaseUrl>> _log;
		
		protected UrlLoggerResults(List<Tuple2<Class<?>, BaseUrl>> log) {
			_log = log;
		}
		
		/**
		 * Verify we have at least one logging call by <clazz>
		 * 
		 * @param clazz
		 * @return
		 */
		public UrlLoggerResults assertLogging(Class<?> clazz) {
			for (Tuple2<Class<?>, BaseUrl> entry : _log) {
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
			for (Tuple2<Class<?>, BaseUrl> entry : _log) {
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
		
		public UrlLoggerResults assertLoggedBy(Class<?> clazz, BaseUrl url, int numCalls) {
			int foundCalls = 0;
			for (Tuple2<Class<?>, BaseUrl> entry : _log) {
				if (entry.f0.equals(clazz) && entry.f1.equals(url)) {
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
		
		/**
		 * Verify we have no URLs logged by <clazz>.
		 * 
		 * @param clazz
		 * @return
		 */
		public UrlLoggerResults assertNotCalledBy(Class<?> clazz) {
			for (Tuple2<Class<?>, BaseUrl> entry : _log) {
				if (entry.f0.equals(clazz)) {
					fail(String.format("Found URL '%s' logged by %s", entry.f1, clazz));
				}
			}
			
			return this;
		}
		

		public UrlLoggerResults assertUrlLogged(BaseUrl url) {
			for (Tuple2<Class<?>, BaseUrl> entry : _log) {
				if (entry.f1.equals(url)) {
					return this;
				}
			}
			
			fail(String.format("Didn't find any logging of URL '%s'", url));
			
			// Keep Eclipse happy
			return this;
		}
		
		public UrlLoggerResults assertUrlNotLoggedBy(Class<?> clazz, BaseUrl url) {
			for (Tuple2<Class<?>, BaseUrl> entry : _log) {
				if (entry.f0.equals(clazz) && entry.f1.equals(url)) {
					fail(String.format("Found URL '%s' logged by %s", url, clazz));
				}
			}
			
			return this;
		}
		

	}
	
}
