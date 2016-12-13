package com.scaleunlimited.flinkcrawler.pojos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.scaleunlimited.flinkcrawler.crawldb.IPayload;
import com.scaleunlimited.flinkcrawler.utils.HashUtils;


@SuppressWarnings("serial")
public class CrawlStateUrl extends BaseUrl implements IPayload {

	// Data needed in-memory for CrawlDB merging
	private FetchStatus _status;		// TODO make this an enum ?
	
	// Data kept in the CrawlDB on-disk payload
	private String _url;
	private String _pld;
	private float _actualScore;			// TODO do we maintain separate page and link scores ?
	private float _estimatedScore;
	private long _lastFetchedTime;
	private long _nextFetchTime;

	public CrawlStateUrl(String url, FetchStatus status, String pld, float actualScore, float estimatedScore, long lastFetchedTime, long nextFetchTime) {
		super();

		_url = url;
		_status = status;
		_pld = pld;
		_actualScore = actualScore;
		_estimatedScore = estimatedScore;
		_lastFetchedTime = lastFetchedTime;
		_nextFetchTime = nextFetchTime;
	}

	@Override
	public String getPartitionKey() {
		return _pld;
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

	@Override
	public String toString() {
		// TODO add more fields to the response.
		return String.format("%s (%s)", _url, _status);
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
		out.writeUTF(_url);
		out.writeUTF(_pld);
		out.writeFloat(_actualScore);
		out.writeFloat(_estimatedScore);
		out.writeLong(_lastFetchedTime);
		out.writeLong(_nextFetchTime);
	}

	// Read the payload fields in
	@Override
	public void readFields(DataInput in) throws IOException {
		_url = in.readUTF();
		_pld = in.readUTF();
		_actualScore = in.readFloat();
		_estimatedScore = in.readFloat();
		_lastFetchedTime = in.readLong();
		_nextFetchTime = in.readLong();
	}

	// Clear the payload fields
	@Override
	public void clear() {
		_url = null;
		_pld = null;
		_actualScore = 0.0f;
		_estimatedScore = 0.0f;
		_lastFetchedTime = 0;
		_nextFetchTime = 0;
	}

}