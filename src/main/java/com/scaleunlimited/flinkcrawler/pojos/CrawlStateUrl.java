package com.scaleunlimited.flinkcrawler.pojos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.scaleunlimited.flinkcrawler.crawldb.IPayload;
import com.scaleunlimited.flinkcrawler.utils.HashUtils;


@SuppressWarnings("serial")
public class CrawlStateUrl extends ValidUrl implements IPayload {

	// Data needed in-memory for CrawlDB merging
	private FetchStatus _status;		// TODO make this an enum ?
	
	// Data kept in the CrawlDB on-disk payload
	private float _actualScore;			// TODO do we maintain separate page and link scores ?
	private float _estimatedScore;
	private long _lastFetchedTime;
	private long _nextFetchTime;

	public CrawlStateUrl(FetchUrl url, FetchStatus status, long nextFetchTime) {
		this(url, status, url.getActualScore(), url.getEstimatedScore(), 0, nextFetchTime);
	}
	
	public CrawlStateUrl(ValidUrl url, FetchStatus status, float actualScore, float estimatedScore, long lastFetchedTime, long nextFetchTime) {
		super(url);

		_status = status;
		_actualScore = actualScore;
		_estimatedScore = estimatedScore;
		_lastFetchedTime = lastFetchedTime;
		_nextFetchTime = nextFetchTime;
	}

	public long makeKey() {
		return HashUtils.longHash(getUrl());
	}

	public FetchStatus getStatus() {
		return _status;
	}
	public void setStatus(FetchStatus status) {
		_status = status;
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

	@Override
	public String toString() {
		// TODO add more fields to the response.
		return String.format("%s (%s)", getUrl(), _status);
	}

	/**
	 * Return in a new object all the fields that we need for merging one CrawlStateUrl
	 * with another one in the CrawlDB DrumMap.
	 *  
	 * @return the new object.
	 */
	public Object makeValue() {
		return _status;
	}

	// write the payload fields out
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeFloat(_actualScore);
		out.writeFloat(_estimatedScore);
		out.writeLong(_lastFetchedTime);
		out.writeLong(_nextFetchTime);
	}

	// Read the payload fields in
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		_actualScore = in.readFloat();
		_estimatedScore = in.readFloat();
		_lastFetchedTime = in.readLong();
		_nextFetchTime = in.readLong();
	}

	// Clear the payload fields
	@Override
	public void clear() {
		super.clear();
		
		_actualScore = 0.0f;
		_estimatedScore = 0.0f;
		_lastFetchedTime = 0;
		_nextFetchTime = 0;
	}

}