package com.scaleunlimited.flinkcrawler.utils;

import java.nio.charset.StandardCharsets;

public class HashUtils {

	public static long longHash(String s) {
		byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
		return getLongHash(bytes, 0, bytes.length);
	}
	
    /**
     * Generate a 64-bit JOAAT hash from the given byte array
     * 
     * @param b Bytes to hash
     * @param offset starting offset
     * @param length number of bytes to hash
     * @return 64-bit hash
     */
    private static long getLongHash(byte[] b, int offset, int length) {
        long result = 0;

        for (int i = 0; i < length; i++) {
            byte curByte = b[offset + i];
            int h = (int)curByte;
            
            result += h & 0x0FFL;
            result += (result << 20);
            result ^= (result >> 12);
        }
        
        result += (result << 6);
        result ^= (result >> 22);
        result += (result << 30);

        return result;
    }

}
