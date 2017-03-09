package com.scaleunlimited.flinkcrawler.crawldb;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class BaseCrawlDBBuilder<T extends BaseCrawlDB> implements Serializable {

	public abstract T build();
}
