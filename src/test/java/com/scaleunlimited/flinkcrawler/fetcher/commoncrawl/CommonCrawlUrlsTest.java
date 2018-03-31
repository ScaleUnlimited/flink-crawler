package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.scaleunlimited.flinkcrawler.urls.BaseUrlNormalizer;
import com.scaleunlimited.flinkcrawler.urls.SimpleUrlNormalizer;

public class CommonCrawlUrlsTest {

    @Test
    public void testConvertToIndexFormat() throws Exception {
        assertEquals("com,domain)/", reverseIt("http://domain.com"));
        assertEquals("com,domain)/", reverseIt("http://www.domain.com"));
        assertEquals("com,domain)/", reverseIt("https://www.domain.com"));
        assertEquals("com,domain,sub)/", reverseIt("http://sub.domain.com"));
        assertEquals("com,domain:8080)/", reverseIt("http://domain.com:8080"));
        assertEquals("com,domain)/path/to/file", reverseIt("http://domain.com/path/to/file"));
        assertEquals("com,domain)/?q=x", reverseIt("http://domain.com?q=x"));
        assertEquals("com,domain)/?q=x", reverseIt("http://domain.com/?q=x"));
        assertEquals("com,domain)/path/to/file?q=x",
                reverseIt("http://domain.com/path/to/file?q=x"));
    }

    @Test
    public void testQueryNormalization() throws Exception {
        assertEquals("com,domain)/path?mode=m&position=123", reverseIt("http://domain.com/path?position=123&mode=M"));
    }
    
    @Test
    public void testPathNormalization() throws Exception {
        assertEquals("com,domain)/path", reverseIt("http://domain.com/path/"));
        assertEquals("com,domain)/%d8%ba%8e%dx%e", reverseIt("http://domain.com/%D8%BA%8E%Dx%E"));
    }
    
    // Enable this test to run through all entries in a (previously downloaded) CDX file,
    // comparing the actual URL in the JSON (converted to SURT format) to the 
    @Ignore
    @Test
    public void testConversionFromCdxFile() throws Exception {
        // 0,124,148,146)/index.php 20170429211342 {"url": "http://146.148.124.0/index.php", "mim....
        final Pattern CDX_LINE_PATTERN = Pattern
                .compile("(.+?)[ ]+(\\d+)[ ]+(\\{.+\\})");

        final File cdxFile = new File("/Users/kenkrugler/Downloads/cdx-00156.gz");

        int numDifferent = 0;
        JsonParser jsonParser = new JsonParser();
        SimpleUrlNormalizer normalizer = new SimpleUrlNormalizer();
        try (BufferedReader lineReader = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(new FileInputStream(cdxFile)),
                        StandardCharsets.UTF_8))) {

            String line;
            while ((line = lineReader.readLine()) != null) {
                // 0,124,148,146)/index.php 20170429211342 {"url": "http://146.148.124.0/index.php", "mim....
                // LOGGER.debug(line);

                Matcher m = CDX_LINE_PATTERN.matcher(line);
                if (!m.matches()) {
                    throw new IOException("Invalid CDX line: " + line);
                }

                String entryKey = m.group(1);
                
                // Sometimes the entry key doesn't represent a fully normalized URL, e.g. it looks like:
                // com,swellinfo)/tropical/index.html?anim=1
                // but the actual URL doesn't have the "index.html", which gets normalized away.
                // So we'll recreate the URL from the entry, normalize it, and use that for comparison
                // TODO - ping Common Crawl about this issue.
                String normalizedEntryKey = normalizeIndexKey(normalizer, entryKey);
                
                String json = m.group(3);
                JsonObject result = jsonParser.parse(json).getAsJsonObject();
                URL url = new URL(normalizer.normalize(result.get("url").getAsString()));
                String indexKey = CommonCrawlUrls.convertToIndexFormat(url);
                if (!indexKey.equals(normalizedEntryKey)) {
                    numDifferent += 1;
                    if (numDifferent < 100) {
                        System.out.println(url);
                        System.out.println(entryKey);
                        System.out.println(normalizedEntryKey);
                        System.out.println(indexKey);
                        System.out.println();
                    } else if ((numDifferent % 100) == 0) {
                        System.out.print('.');
                    }
                }
            }
        }
    }
    
    private String normalizeIndexKey(BaseUrlNormalizer normalizer, String entryKey) throws MalformedURLException {
        // com,swellinfo)/tropical/index.html?anim=1&config=&forecast=pass&pass=tropsatellites&region=watl&sattype=bwir

        int domainSplit = entryKey.indexOf(')');
        if (domainSplit == -1) {
            throw new IllegalArgumentException("Invalid entry key: " + entryKey);
        }
        
        String reversedDomain = entryKey.substring(0, domainSplit);
        int portSplit = reversedDomain.indexOf(':');
        int portNumber = -1;
        if (portSplit != -1) {
            portNumber = Integer.parseInt(reversedDomain.substring(portSplit + 1));
            reversedDomain = reversedDomain.substring(0, portSplit);
        }
        
        StringBuilder realDomain = new StringBuilder();
        String[] domainParts = reversedDomain.split(",");
        for (int i = domainParts.length - 1; i >= 0; i--) {
            String domainPart = domainParts[i];
            if (realDomain.length() > 0) {
                realDomain.append('.');
            }
            
            realDomain.append(domainPart);
        }
        
        URL realUrl = new URL("http", realDomain.toString(), portNumber, entryKey.substring(domainSplit + 1));
        URL normalizedUrl = new URL(normalizer.normalize(realUrl.toExternalForm()));
        
        return CommonCrawlUrls.convertToIndexFormat(normalizedUrl);
    }

    private String reverseIt(String url) throws MalformedURLException {
        return CommonCrawlUrls.convertToIndexFormat(new URL(url));
    }


}
