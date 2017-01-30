package com.scaleunlimited.flinkcrawler.utils;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple3;

import com.scaleunlimited.flinkcrawler.pojos.BaseUrl;

public class UrlLogger {

	private static IUrlLogger LOGGER;
	
	static {
		// Load the logger, but only if the implementation is available.
		try {
			Class<?> clazz = UrlLogger.class.getClassLoader().loadClass("com.scaleunlimited.flinkcrawler.utils.UrlLoggerImpl");
			LOGGER = (IUrlLogger)clazz.newInstance();
		} catch (Exception e) {
			// Ignore - no logger available.
			System.err.println("Can't load logger: " + e.getMessage());
		}
	}
	
	public static void record(Class<?> clazz, BaseUrl url, String... metaData) {
		if (LOGGER != null) {
			LOGGER.record(clazz, url, metaData);
		}
	}

	public static List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> getLog() {
		if (LOGGER != null) {
			return LOGGER.getLog();
		} else {
			throw new IllegalStateException("No URL logging enabled");
		}
	}
	
}
