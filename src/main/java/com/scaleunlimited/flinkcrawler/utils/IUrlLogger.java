package com.scaleunlimited.flinkcrawler.utils;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple3;

import com.scaleunlimited.flinkcrawler.pojos.BaseUrl;

public interface IUrlLogger {

	public void record(Class<?> clazz, BaseUrl url, String... metaData);
	
	public void record(Class<?> clazz, BaseUrl url, Map<String, String> metaData);

	public void clear();
	
	public List<Tuple3<Class<?>, String, Map<String, String>>> getLog();

}
