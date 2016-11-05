package com.scaleunlimited.flinkcrawler.pojos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.scaleunlimited.flinkcrawler.crawldb.Payload;
import com.scaleunlimited.flinkcrawler.utils.HashUtils;


public class CrawlStateUrl {

	private String _url;
	private FetchStatus _status;		// TODO make this an enum ?
	private String _pld;
	private float _actualScore;	// TODO do we maintain separate page and link scores ?
	private float _estimatedScore;
	private long _lastFetchedTime;
	private long _nextFetchTime;
	
	public CrawlStateUrl(String url, FetchStatus status, String pld, float actualScore, float estimatedScore, long lastFetchedTime, long nextFetchTime) {
		_url = url;
		_status = status;
		_pld = pld;
		_actualScore = actualScore;
		_estimatedScore = estimatedScore;
		_lastFetchedTime = lastFetchedTime;
		_nextFetchTime = nextFetchTime;
	}
	
	public String getUrl() {
		return _url;
	}
	public void setUrl(String url) {
		_url = url;
	}
	
	public long makeKey() {
		return HashUtils.longHash(_url);
	}
	
	public FetchStatus getStatus() {
		return _status;
	}
	public void setStatus(FetchStatus status) {
		_status = status;
	}
	
	public String getPLD() {
		return _pld;
	}
	
	public void setPLD(String pld) {
		_pld = pld;
	}
	
	public float getActualScore() {
		return _actualScore;
	}
	
	public void setActualScore(float actualScore) {
		_actualScore = actualScore;
	}
	
	public float getEstimatedScore() {
		return _estimatedScore;
	}
	
	public void setEstimatedScore(float estimatedScore) {
		_estimatedScore = estimatedScore;
	}
	
	public float getLastFetchedTime() {
		return _lastFetchedTime;
	}
	
	public void setLastFetchedTime(long lastFetchedTime) {
		_lastFetchedTime = lastFetchedTime;
	}
	
	public float getNextFetchTime() {
		return _nextFetchTime;
	}
	
	public void setNextFetchTime(long nextFetchTime) {
		_nextFetchTime = nextFetchTime;
	}

	/**
	 * Return in a new object all the fields that we need for merging one CrawlStateUrl
	 * with another one in the CrawlDB DrumMap.
	 *  
	 * @return the new object.
	 */
	public Object makeValue() {
		return null;
	}

	/**
	 * Return a new paylood that contains all of the fields which are not needed to
	 * merge these objects, and thus can be kept on disk and out of memory.
	 * @return
	 */
	public Payload makePayload() {
		return new CrawlStateUrlPayload(_url);
	}
	
	@Override
	public String toString() {
		// TODO add more fields to the response.
		return _url;
	}
	
	public static class CrawlStateUrlPayload extends Payload {

		private String _url;
		
		public CrawlStateUrlPayload() { }
		
		public CrawlStateUrlPayload(String url) {
			_url = url;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(_url);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			_url = in.readUTF();
		}

		@Override
		public void clear() {
			_url = null;
		}
		
	}
	
}