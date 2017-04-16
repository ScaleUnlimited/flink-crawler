package com.scaleunlimited.flinkcrawler.pojos;

import java.io.IOException;
import java.io.RandomAccessFile;

import com.scaleunlimited.flinkcrawler.crawldb.DrumKeyValue;
import com.scaleunlimited.flinkcrawler.utils.ByteUtils;
import com.scaleunlimited.flinkcrawler.utils.HashUtils;


@SuppressWarnings("serial")
public class CrawlStateUrl extends ValidUrl {

	// Status = 2 bytes, status time = 8 bytes.
	private static final int HAS_VALUE_LENGTH = 2 + 8;
	
	// Data needed in-memory for CrawlDB merging
	private FetchStatus _status;		// TODO make this an enum ?
	
	// Data kept in the CrawlDB on-disk payload
	private float _actualScore;			// TODO do we maintain separate page and link scores ?
	private float _estimatedScore;
	private long _statusTime;
	private long _nextFetchTime;

	public CrawlStateUrl() {
		// For creating from payload
	}
	
	public CrawlStateUrl(FetchUrl url, FetchStatus status, long nextFetchTime) {
		this(url, status, url.getActualScore(), url.getEstimatedScore(), System.currentTimeMillis(), nextFetchTime);
	}
	
	public CrawlStateUrl(ValidUrl url, FetchStatus status, float actualScore, float estimatedScore, long statusTime, long nextFetchTime) {
		super(url);

		_status = status;
		_actualScore = actualScore;
		_estimatedScore = estimatedScore;
		_statusTime = statusTime;
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

	public long getStatusTime() {
		return _statusTime;
	}

	public void setStatusTime(long statusTime) {
		_statusTime = statusTime;
	}

	public float getNextFetchTime() {
		return _nextFetchTime;
	}

	public void setNextFetchTime(long nextFetchTime) {
		_nextFetchTime = nextFetchTime;
	}

	/**
	 * We have an array of bytes (with first byte = length) that
	 * is coming from the result of merging entries in the CrawlDB.
	 * We need to update the fields that are saved in this value.
	 * 
	 * @param value
	 */
	public void setFromValue(byte[] value) {
		int valueLength = DrumKeyValue.getValueLength(value);
		
		if (valueLength == 0) {
			_status = FetchStatus.UNFETCHED;
			_statusTime = 0;
		} else if (valueLength < HAS_VALUE_LENGTH) {
			throw new IllegalArgumentException(String.format("Length of value must be 0 or %d, got %d", HAS_VALUE_LENGTH, valueLength));
		} else {
			// TODO handle additional status values.
			_status = FetchStatus.values()[ByteUtils.bytesToShort(value, 1)];
			_statusTime = ByteUtils.bytesToLong(value, 3);
		}
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
	 * @return the buffer.
	 */
	public byte[] getValue(byte[] value) {
		if (_status == FetchStatus.UNFETCHED) {
			value[0] = 0;
		} else {
			// TODO set up other values as needed.
			value[0] = HAS_VALUE_LENGTH;
			ByteUtils.shortToBytes((short)_status.ordinal(), value, 1);
			ByteUtils.longToBytes(_statusTime, value, 3);
		}
		
		return value;
	}

	// TODO have everyone use DrumMap.MAX_VALUE_SIZE vs. calling this + 1.
	public static int maxValueLength() {
		return HAS_VALUE_LENGTH;
	}
	
	public static int averageValueLength() {
		// TODO - figure out empirical value for this. Somewhere between 0 (unfetched) and this value.
		return HAS_VALUE_LENGTH;
	}
	
	public static FetchStatus getFetchStatus(byte[] value) {
		int valueLength = DrumKeyValue.getValueLength(value);
		
		if (valueLength == 0) {
			return FetchStatus.UNFETCHED;
		} else if (valueLength < 2) {
			throw new IllegalArgumentException("Length of value must be 0 or >= 2, got " + valueLength);
		} else {
			return FetchStatus.values()[ByteUtils.bytesToShort(value, 1)];
		}
	}
	
	public static long getFetchStatusTime(byte[] value) {
		int valueLength = DrumKeyValue.getValueLength(value);
		
		if (valueLength == 0) {
			return 0;
		} else if (valueLength != HAS_VALUE_LENGTH) {
			throw new IllegalArgumentException(String.format("Length of value must be 0 or %d, got %d", HAS_VALUE_LENGTH, valueLength));
		} else {
			return ByteUtils.bytesToLong(value, 3);
		}
	}
	
	// Clear the payload fields
	@Override
	public void clear() {
		super.clear();
		
		_actualScore = 0.0f;
		_estimatedScore = 0.0f;
		_statusTime = 0;
		_nextFetchTime = 0;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Float.floatToIntBits(_actualScore);
		result = prime * result + Float.floatToIntBits(_estimatedScore);
		result = prime * result
				+ (int) (_statusTime ^ (_statusTime >>> 32));
		result = prime * result
				+ (int) (_nextFetchTime ^ (_nextFetchTime >>> 32));
		result = prime * result + ((_status == null) ? 0 : _status.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		CrawlStateUrl other = (CrawlStateUrl) obj;
		if (Float.floatToIntBits(_actualScore) != Float
				.floatToIntBits(other._actualScore))
			return false;
		if (Float.floatToIntBits(_estimatedScore) != Float
				.floatToIntBits(other._estimatedScore))
			return false;
		if (_statusTime != other._statusTime)
			return false;
		if (_nextFetchTime != other._nextFetchTime)
			return false;
		if (_status != other._status)
			return false;
		return true;
	}

	public static CrawlStateUrl fromKV(DrumKeyValue dkv, RandomAccessFile payloadFile) throws IOException {
		CrawlStateUrl result = new CrawlStateUrl();
		
		// Get the URL
		payloadFile.seek(dkv.getPayloadOffset());
		result.readFields(payloadFile);
		
		// Get the other fields
		result.setFromValue(dkv.getValue());
		
		return result;
	}

	
}