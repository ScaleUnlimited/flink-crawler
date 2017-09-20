package com.scaleunlimited.flinkcrawler.utils;

public class ByteUtils {

	public static void longToBytes(long value, byte[] dest, int destOffset) {
		writeBytes(value, dest, destOffset, 8);
	}

	public static void intToBytes(int value, byte[] dest, int destOffset) {
		writeBytes(value, dest, destOffset, 4);
	}

	public static void shortToBytes(short value, byte[] dest, int destOffset) {
		writeBytes(value, dest, destOffset, 2);
	}

	public static void floatToBytes(float value, byte[] dest, int destOffset) {
		writeBytes((int)value, dest, destOffset, 4);
	}

	private static void writeBytes(long value, byte[] dest, int destOffset, int valueSize) {
		destOffset += valueSize;
		for (int i = 0; i < valueSize; i++) {
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

	public static int bytesToUnsignedByte(byte[] src, int srcOffset) {
		return src[srcOffset] & 0x00FF;
	}
	
	public static short bytesToShort(byte[] src, int srcOffset) {
		short result = 0;
		for (int i = 0; i < 2; i++) {
			result <<= 8;
			result |= (src[srcOffset++] & 0x00FF);
		}
		
		return result;
	}

	public static int bytesToInt(byte[] src, int srcOffset) {
		int result = 0;
		for (int i = 0; i < 4; i++) {
			result <<= 8;
			result |= (src[srcOffset++] & 0x00FF);
		}
		
		return result;
	}

}
