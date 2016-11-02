package com.scaleunlimited.flinkcrawler.crawldb;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.scaleunlimited.flinkcrawler.pojos.CrawlStateUrl;
import com.scaleunlimited.flinkcrawler.pojos.FetchUrl;

@SuppressWarnings("serial")
public class SimpleCrawlDB extends BaseCrawlDB {

	private ConcurrentLinkedQueue<FetchUrl> _urls;

	@Override
	public void open() {
		_urls = new ConcurrentLinkedQueue<>();
	}

	@Override
	public void close() {
	}

	@Override
	public void add(CrawlStateUrl url) {
		_urls.add(new FetchUrl(url.getUrl(), url.getEstimatedScore(), url.getPLD(), url.getActualScore()));
	}

	@Override
	public FetchUrl get() {
		return _urls.poll();
	}

}
