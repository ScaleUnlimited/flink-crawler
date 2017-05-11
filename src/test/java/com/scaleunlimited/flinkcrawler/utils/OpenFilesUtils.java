package com.scaleunlimited.flinkcrawler.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class OpenFilesUtils {

	private List<File> _reservedFiles;
	private List<OutputStream> _openStreams;
	
	public OpenFilesUtils() throws IOException {
		_reservedFiles = new ArrayList<>();
		_openStreams = new ArrayList<>();
		
		for (int i = 0; i < 100; i++) {
			File file = File.createTempFile("flink-crawler", ".bin");
			_reservedFiles.add(file);
		}
		
		openStreams();
	}
	
	private void openStreams() {
		for (File file : _reservedFiles) {
			try {
				_openStreams.add(new FileOutputStream(file));
			} catch (FileNotFoundException e) {
				throw new RuntimeException("Error opening reserved file stream", e);
			}
		}
	}

	private void closeStreams() {
		for (OutputStream s : _openStreams) {
			IOUtils.closeQuietly(s);
		}
		
		_openStreams.clear();
	}

	/**
	 * Utility to log all open files under <directory>
	 * 
	 * @param directory
	 */
	
	public List<String> logOpenFiles(File directory) {
		String dirName = null;
		
		try {
			dirName = directory.getCanonicalPath();
		} catch (IOException e) {
			throw new RuntimeException("Error getting path", e);
		}
		
		return logOpenFiles(dirName);
	}
	
	public List<String> logOpenFiles(String directory) {
		
		// System.out.println("Finding open files in " + directory);
		
		// String[] cmd = new String[] {"lsof", "+D", directory};
		List<String> result = new ArrayList<>();
		String[] cmd = new String[] {"lsof"};
		ProcessBuilder builder = new ProcessBuilder(cmd);
		builder.redirectOutput(Redirect.PIPE);
		
		try {
			closeStreams();
			
			Process p = builder.start();
			List<String> files = org.apache.commons.io.IOUtils.readLines(p.getInputStream());
			for (String file : files) {
				// System.out.println(file);
				if (file.contains(directory)) {
					result.add(file);
				}
			}
			
			return result;
		} catch (IOException e) {
			throw new RuntimeException("Error generating list of open files", e);
		} finally {
			openStreams();
		}
		
	}

}
