package com.scaleunlimited.flinkcrawler.pojos;

import java.io.IOException;
import java.io.RandomAccessFile;

import com.scaleunlimited.flinkcrawler.crawldb.DrumKeyValue;
import com.scaleunlimited.flinkcrawler.utils.ByteUtils;
import com.scaleunlimited.flinkcrawler.utils.HashUtils;


@SuppressWarnings("serial")
public class CrawlStateUrl extends ValidUrl {

	// Status = 2 bytes, status time = 4 bytes, fetch time = 4 bytes,
	// score = 2 bytes
	private static final int HAS_VALUE_LENGTH = 2 + 4 + 4 + 2;
	
	private static final float MAX_FRACTIONAL_SCORE = (float)((short)0x0FFF);
	
	// all bits set in 4.12 format
	private static final float MAX_SCORE = 15.99975586f;
			
	// Data needed in-memory for CrawlDB merging
	private FetchStatus _status;		// TODO make this an enum ?
	
	// Data kept in the CrawlDB on-disk payload
	private float _score;
	private long _statusTime;
	private long _nextFetchTime;

	public CrawlStateUrl() {
		// For creating from payload
	}
	
	public CrawlStateUrl(FetchUrl url, FetchStatus status, long nextFetchTime) {
		this(url, status, url.getScore(), System.currentTimeMillis(), nextFetchTime);
	}
	
	public CrawlStateUrl(ValidUrl url, FetchStatus status, float score, long statusTime, long nextFetchTime) {
		super(url);

		_status = status;
		_score = score;
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

	public float getScore() {
		return _score;
	}

	public void setScore(float score) {
		_score = score;
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
		
		if (valueLength != HAS_VALUE_LENGTH) {
			throw new IllegalArgumentException(String.format("Length of value must be %d, got %d", HAS_VALUE_LENGTH, valueLength));
		} else {
			int offset = 1;
			_status = FetchStatus.values()[ByteUtils.bytesToShort(value, offset)];
			offset += 2;
			_statusTime = getTimeFromBytes(value, offset);
			offset += 4;
			_nextFetchTime = getTimeFromBytes(value, offset);
			offset += 4;
			_score = getScoreFromBytes(value, offset);
			offset += 2;
		}
	}
	
	private float getScoreFromBytes(byte[] value, int offset) {
		short scoreAsShort = ByteUtils.bytesToShort(value, offset);
		
		// High 4 bits are integral value (0..15), low 12 bits are fractional
		int integralScore = (scoreAsShort >> 12) & 0x000F;
		int fractionalScore = scoreAsShort & 0x0FFF;
		
		return integralScore + fractionalScore/MAX_FRACTIONAL_SCORE;
	}

	private long getTimeFromBytes(byte[] value, int offset) {
		int timeInSeconds = ByteUtils.bytesToInt(value, offset);
		return timeInSeconds * 1000L;
	}

	private int timeToInt(long time) {
		return (int)(time / 1000L);
	}
	
	private short scoreToShort(float score) {
		if (score > MAX_SCORE) {
			score = MAX_SCORE;
		} else if (score < 0.0) {
			score = 0.0f;
		}

		float fractionalPart = score % 1;
		float integralPart = score - fractionalPart;

		int result = ((int)integralPart << 12) | (int)(fractionalPart * (MAX_FRACTIONAL_SCORE + 1));
		return (short)result;
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
		int offset = 0;
		value[offset] = HAS_VALUE_LENGTH;
		offset += 1;
		ByteUtils.shortToBytes((short)_status.ordinal(), value, offset);
		offset += 2;
		ByteUtils.intToBytes(timeToInt(_statusTime), value, offset);
		offset += 4;
		ByteUtils.intToBytes(timeToInt(_nextFetchTime), value, offset);
		offset += 4;
		ByteUtils.shortToBytes(scoreToShort(_score), value, offset);
		
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
		
		_score = 0.0f;
		_statusTime = 0;
		_nextFetchTime = 0;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Float.floatToIntBits(_score);
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
		if (Float.floatToIntBits(_score) != Float
				.floatToIntBits(other._score))
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