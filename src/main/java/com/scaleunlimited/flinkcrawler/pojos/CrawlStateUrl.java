package com.scaleunlimited.flinkcrawler.pojos;

import java.net.MalformedURLException;

import com.scaleunlimited.flinkcrawler.utils.ByteUtils;
import com.scaleunlimited.flinkcrawler.utils.HashUtils;


/**
 * The CrawlStateUrl is the fundamental unit of state in the CrawlDB. It consists of the
 * actual URL, plus other fields necessary to handle merging of URLs and prioritizing of
 * URLs to be fetched.
 * 
 */
@SuppressWarnings("serial")
public class CrawlStateUrl extends ValidUrl {

	private static final int LENGTH_OFFSET = 0;
	private static final int STATUS_OFFSET = LENGTH_OFFSET + 1;
	private static final int STATUS_TIME_OFFSET = STATUS_OFFSET + 1;
	private static final int SCORE_OFFSET = STATUS_TIME_OFFSET + 4;
	private static final int FETCH_TIME_OFFSET = SCORE_OFFSET + 2;
	
	public static final int VALUE_LENGTH = FETCH_TIME_OFFSET + 4;
	public static final int VALUE_SIZE = VALUE_LENGTH + 1;

	private static final float MAX_FRACTIONAL_SCORE = (float)((short)0x0FFF);
	
	// all bits set in 4.12 format for scores.
	private static final float MAX_SCORE = 15.99975586f;
			
	// Data needed in-memory for CrawlDB merging
	private FetchStatus _status;
	private long _statusTime;
	private float _score;
	private long _nextFetchTime;
	
	// What type of URL we've got. URLs in the crawlDB will only be
	// of type validated, others are transient state.
	private UrlType _urlType;
	private int _id;
	
	// Payload has all of the above fields, plus the URL.
	
	public static CrawlStateUrl makeTicklerUrl(int id) {
		return new CrawlStateUrl(UrlType.TICKLER, id);
	}

	public static CrawlStateUrl makeDomainUrl(String domain) throws MalformedURLException {
		return new CrawlStateUrl(UrlType.DOMAIN, domain);
	}

	public CrawlStateUrl() {
		// For creating from payload
	}
	
	public CrawlStateUrl(UrlType urlType, int id) {
		_urlType = urlType;
		_id = id;
	}
	
	public CrawlStateUrl(UrlType urlType, String domain) throws MalformedURLException {
		super(new ValidUrl("http://" + domain));
		_urlType = urlType;
	}
	
	public CrawlStateUrl(FetchUrl url, FetchStatus status, long nextFetchTime) {
		this(url, status, System.currentTimeMillis(), url.getScore(), nextFetchTime);
	}
	
	public CrawlStateUrl(ValidUrl url, FetchStatus status, long statusTime, float score, long nextFetchTime) {
		super(url);

		_status = status;
		_score = score;
		_statusTime = statusTime;
		_nextFetchTime = nextFetchTime;
		
		_urlType = UrlType.REGULAR;
	}

	public long makeKey() {
		return HashUtils.longHash(getUrl());
	}

	/* (non-Javadoc)
	 * @see com.scaleunlimited.flinkcrawler.pojos.ValidUrl#getPartitionKey()
	 * 
	 * For special URLs, we have to return the the id, so that we can ensure
	 * every CrawlDBFunction gets a regular tickler.
	 */
	@Override
	public Integer getPartitionKey() {
		if ((_urlType == UrlType.REGULAR) || (_urlType == UrlType.DOMAIN)) {
			return super.getPartitionKey();
		} else {
			return _id;
		}
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

	public long getNextFetchTime() {
		return _nextFetchTime;
	}

	public void setNextFetchTime(long nextFetchTime) {
		_nextFetchTime = nextFetchTime;
	}

	public UrlType getUrlType() {
		return _urlType;
	}
	
	public void setUrlType(UrlType urlType) {
		_urlType = urlType;
	}
	
	/**
	 * We have an array of bytes (with first byte = length) that
	 * is coming from the result of merging entries in the CrawlDB.
	 * We need to update the fields that are saved in this value.
	 * 
	 * @param value
	 */
	public void setFromValue(byte[] value) {
		_status = getFetchStatus(value);
		_statusTime = getStatusTime(value);
		_score = getScore(value);
		_nextFetchTime = getFetchTime(value);
		
		// A persisted URL is always validated.
		// setUrlType(UrlType.VALIDATED);
	}
	
	// TODO move into ByteUtils?
	private static float getScoreFromBytes(byte[] value, int offset) {
		short scoreAsShort = ByteUtils.bytesToShort(value, offset);
		
		// High 4 bits are integral value (0..15), low 12 bits are fractional
		int integralScore = (scoreAsShort >> 12) & 0x000F;
		int fractionalScore = scoreAsShort & 0x0FFF;
		
		return integralScore + fractionalScore/MAX_FRACTIONAL_SCORE;
	}

	// TODO move into ByteUtils?
	private static long getTimeFromBytes(byte[] value, int offset) {
		int timeInSeconds = ByteUtils.bytesToInt(value, offset);
		return timeInSeconds * 1000L;
	}

	private static int timeToInt(long time) {
		return (int)(time / 1000L);
	}
	
	private static short scoreToShort(float score) {
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
		if (_urlType == UrlType.REGULAR) {
			return String.format("%s (%s)", getUrl(), _status);
		} else {
			return String.format("%s (%d)", _urlType, _id);
		}
	}

	/**
	 * Return in the provided byte array all the fields that we need for merging one CrawlStateUrl
	 * with another one in the CrawlDB DrumMap.
	 * 
	 * @return the buffer.
	 */
	public byte[] getValue(byte[] value) {
		value[LENGTH_OFFSET] = VALUE_LENGTH;
		value[STATUS_OFFSET] = (byte)_status.ordinal();
		ByteUtils.intToBytes(timeToInt(_statusTime), value, STATUS_TIME_OFFSET);
		ByteUtils.shortToBytes(scoreToShort(_score), value, SCORE_OFFSET);
		ByteUtils.intToBytes(timeToInt(_nextFetchTime), value, FETCH_TIME_OFFSET);

		return value;
	}

	public static FetchStatus getFetchStatus(byte[] value) {
		checkValue(value);
		
		return FetchStatus.values()[value[STATUS_OFFSET]];
	}
	
	public static long getStatusTime(byte[] value) {
		checkValue(value);
		
		return getTimeFromBytes(value, STATUS_TIME_OFFSET);
	}
	
	public static float getScore(byte[] value) {
		checkValue(value);
		
		return getScoreFromBytes(value, SCORE_OFFSET);
	}
	
	public static long getFetchTime(byte[] value) {
		checkValue(value);
		
		return getTimeFromBytes(value, FETCH_TIME_OFFSET);
	}
	
	private static void checkValue(byte[] value) {
		int valueLength = (int)value[0] & 0x00FF;
		
		if (valueLength != VALUE_LENGTH) {
			throw new IllegalArgumentException(String.format("Length of value must be %d, got %d", VALUE_LENGTH, valueLength));
		}
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

	public static void setValue(byte[] value, FetchStatus status, long statusTime, float score, long fetchTime) {
		value[LENGTH_OFFSET] = VALUE_LENGTH;
		value[STATUS_OFFSET] = (byte)status.ordinal();
		
		ByteUtils.intToBytes(timeToInt(statusTime), value, STATUS_TIME_OFFSET);
		ByteUtils.shortToBytes(scoreToShort(score), value, SCORE_OFFSET);
		ByteUtils.intToBytes(timeToInt(fetchTime), value, FETCH_TIME_OFFSET);
	}

	
}