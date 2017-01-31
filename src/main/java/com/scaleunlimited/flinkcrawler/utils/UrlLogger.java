package com.scaleunlimited.flinkcrawler.utils;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.BaseUrl;

public class UrlLogger {
	private static final Logger LOGGER = LoggerFactory.getLogger(UrlLogger.class);

	private static IUrlLogger URL_LOGGER;
	
	static {
		// Load the logger, but only if the implementation is available.
		try {
			Class<?> clazz = UrlLogger.class.getClassLoader().loadClass("com.scaleunlimited.flinkcrawler.utils.UrlLoggerImpl");
			URL_LOGGER = (IUrlLogger)clazz.newInstance();
		} catch (Exception e) {
			// Ignore - no logger available.
			LOGGER.error("Can't load logger: " + e.getMessage());
		}
	}
	
	public static void record(Class<?> clazz, BaseUrl url, String... metaData) {
		if (URL_LOGGER != null) {
			URL_LOGGER.record(clazz, url, metaData);
		}
	}

	public static List<Tuple3<Class<?>, BaseUrl, Map<String, String>>> getLog() {
		if (URL_LOGGER != null) {
			return URL_LOGGER.getLog();
		} else {
			throw new IllegalStateException("No URL logging enabled");
		}
	}
	
}
