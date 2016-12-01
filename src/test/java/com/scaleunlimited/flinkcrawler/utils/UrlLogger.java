package com.scaleunlimited.flinkcrawler.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;

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
	
}
