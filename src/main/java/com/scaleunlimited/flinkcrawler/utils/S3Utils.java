package com.scaleunlimited.flinkcrawler.utils;

public class S3Utils {
	
	private static final String S3_PROTOCOL = "s3://";
	
	public static boolean isS3File(String filename) {
		return filename.startsWith(S3_PROTOCOL);
	}

	public static boolean fileExists(String filename) {
		if (!isS3File(filename)) {
			return false;
		}
		
		// TODO make request to see if file exists. Hmm, might need then
		// to make this an object with credentials.
		return false;
	}

	public static String getBucket(String filename) {
		if (!isS3File(filename)) {
			throw new IllegalArgumentException("Not an S3 filename: " + filename);
		}
		
		filename = filename.substring(S3_PROTOCOL.length());
		int pathStart = filename.indexOf('/');
		if (pathStart == -1) {
			throw new IllegalArgumentException("S3 filename doesn't have a path: " + filename);
		}
		
		return filename.substring(0, pathStart);
	}
	
	public static String getPath(String filename) {
		if (!isS3File(filename)) {
			throw new IllegalArgumentException("Not an S3 filename: " + filename);
		}
		
		filename = filename.substring(S3_PROTOCOL.length());
		int pathStart = filename.indexOf('/');
		if (pathStart == -1) {
			throw new IllegalArgumentException("S3 filename doesn't have a path: " + filename);
		}
		
		return filename.substring(pathStart + 1);
	}
}
