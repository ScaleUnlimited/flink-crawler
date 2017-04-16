package com.scaleunlimited.flinkcrawler.utils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.crawldb.DrumKVIterator;


public class IoUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(IoUtils.class);
    
    private IoUtils() {
        // Enforce class isn't instantiated
    }

    public static void safeClose(InputStream is) {
        if (is == null) {
            return;
        }
        
        try {
            is.close();
        } catch (IOException e) {
            LOGGER.warn("IOException closing input stream", e);
        }
    }
    
    public static void safeClose(OutputStream os) {
        if (os == null) {
            return;
        }
        
        try {
            os.close();
        } catch (IOException e) {
            LOGGER.warn("IOException closing input stream", e);
        }
    }
    
	/**
	 * Read one line of input from the console.
	 * 
	 * @return Text that the user entered
	 * @throws IOException
	 */
	public static String readInputLine() throws IOException {
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);
		return br.readLine();
	}

	/**
	 * Close a set of items, and throw the first exception we get
	 * (if any) from these actions. So everything will get closed,
	 * though there might be some exceptions which are hidden.
	 * 
	 * @param items
	 * @throws IOException
	 */
	public static void closeAll(Closeable... items) throws IOException {
		IOException savedException = null;
		
		for (Closeable item : items) {
			try {
				item.close();
			} catch (IOException e) {
				if (savedException == null) {
					savedException = e;
				}
			}
		}
		
		if (savedException != null) {
			throw savedException;
		}
	}

	public static void closeAllQuietly(Closeable... items) {
		for (Closeable item : items) {
			if (item != null) {
				IOUtils.closeQuietly(item);
			}
		}
	}


}
