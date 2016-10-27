package com.scaleunlimited.flinkcrawler.fetcher;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public abstract class BaseFetcher implements Serializable {

	public abstract byte[] fetch(FetchUrl url, Map<String, List<String>> headers);

}
