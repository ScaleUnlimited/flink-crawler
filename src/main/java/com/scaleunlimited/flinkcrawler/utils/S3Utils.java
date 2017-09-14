package com.scaleunlimited.flinkcrawler.utils;

import java.io.InputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Utils {
	
	private static final String S3_PROTOCOL = "s3://";
	
	public static boolean isS3File(String filename) {
		return filename.startsWith(S3_PROTOCOL);
	}

	public static boolean fileExists(String filename) {
		if (!isS3File(filename)) {
			return false;
		}
		
		// TODO It would obviously be better to have some kind of S3Helper object
		// that would have its own AmazonS3 client instance.
		AmazonS3 s3Client = makeS3Client();
		String bucket = getBucket(filename);
		String key = getPath(filename);
		return s3Client.doesObjectExist(bucket, key);
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
	
	public static InputStream makeS3FileStream(String bucketName, String key) {
		AmazonS3 s3Client = makeS3Client();
		S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
		return object.getObjectContent();
	}
	
	public static AmazonS3 makeS3Client() {
		return AmazonS3ClientBuilder.defaultClient();
		
//		AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
//		
//		// http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/EnvironmentVariableCredentialsProvider.html
//		builder.setCredentials(new DefaultAWSCredentialsProviderChain());
//		return builder.build();
	}
}
