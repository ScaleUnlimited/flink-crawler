package com.scaleunlimited.flinkcrawler.utils;

public class ByteUtils {

	public static void longToBytes(long value, byte[] dest, int destOffset) {
		destOffset += 8;
		for (int i = 0; i < 8; i++) {
			dest[--destOffset] = (byte)value;
			value >>= 8;
		}
	}

	public static void intToBytes(int value, byte[] dest, int destOffset) {
		destOffset += 4;
		for (int i = 0; i < 4; i++) {
			dest[--destOffset] = (byte)value;
			value >>= 8;
		}
	}

	public static long bytesToLong(byte[] src, int srcOffset) {
		long result = 0;
		for (int i = 0; i < 8; i++) {
			result <<= 8;
			result |= (src[srcOffset++] & 0x00FFL);
		}
		
		return result;
	}

}
