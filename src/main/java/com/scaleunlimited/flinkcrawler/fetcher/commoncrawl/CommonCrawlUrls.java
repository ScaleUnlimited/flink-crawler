package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class CommonCrawlUrls {

    private static final String ENCODED_CHARS = "0123456789abcdefABCDEF";

    // https://github.com/iipc/webarchive-commons/blob/master/src/main/java/org/archive/url/HandyURL.java
    // http://crawler.archive.org/articles/user_manual/glossary.html#surt
    // https://kris-sigur.blogspot.com/2016/03/rewriting-cdx-file-format.html
    public static String convertToIndexFormat(URL url) {
        StringBuilder reversedUrl = new StringBuilder();

        String domain = url.getHost();
        String[] domainParts = domain.split("\\.");
        for (int i = domainParts.length - 1; i >= 0; i--) {
            // Skip leading www
            if ((i > 0) || !domainParts[i].startsWith("www")) {
                if (reversedUrl.length() > 0) {
                    reversedUrl.append(',');
                }

                reversedUrl.append(domainParts[i]);
            }
        }

        if (url.getPort() != -1) {
            reversedUrl.append(':');
            reversedUrl.append(url.getPort());
        }

        reversedUrl.append(")");

        boolean hasPath = !url.getPath().isEmpty();
        boolean hasQuery = url.getQuery() != null;
        boolean hasHash = url.getRef() != null;
        
        if (hasPath) {
            // reversedUrl.append(lowerCaseEncodedChars(url.getPath()));
            reversedUrl.append(normalizePath(url.getPath()));
        } else if (hasQuery || hasHash) {
            reversedUrl.append('/');
        } else {
            // TODO figure out why we need a trailing '/'
            reversedUrl.append('/');
        }

        if (hasQuery) {
            reversedUrl.append('?');
            reversedUrl.append(normalizeQuery(url.getQuery()));
        }

        if (hasHash) {
            reversedUrl.append('#');
            reversedUrl.append(url.getRef());
        }

        return reversedUrl.toString();
    }
    
    private static String normalizePath(String path) {
        if (path.equals("/")) {
            return path;
        }
        
        return path.toLowerCase(Locale.ROOT).replaceAll("/$", "");
    }
    
    private static String normalizeQuery(String query) {
        StringBuilder result = new StringBuilder();
        Map<String, String> paramMap = new HashMap<>();
        List<String> keys = new ArrayList<>();
        String[] params = query.toLowerCase(Locale.ROOT).split("&");
        for (String param : params) {
            String[] pieces = param.split("=", 1);
            keys.add(pieces[0]);
            paramMap.put(pieces[0], param);
        }
        
        Collections.sort(keys);
        for (String key : keys) {
            if (result.length() > 0) {
                result.append('&');
            }
            result.append(paramMap.get(key));
        }
        
        return result.toString();
    }

    /**
     * If the path contains URL-encoded characters, we need to make sure A-F are lowercased.
     * 
     * @param path
     * @return
     */
    private static String lowerCaseEncodedChars(String path) {
        int offset = path.indexOf('%');
        if (offset == -1) {
            return path;
        }

        boolean inPercent = true;
        boolean inFirstDigit = false;
        char savedFirstDigit = ' ';

        offset += 1;
        StringBuilder result = new StringBuilder(path.substring(0, offset));

        for (; offset < path.length(); offset++) {
            char c = path.charAt(offset);
            if (!inPercent) {
                result.append(c);
                if (c == '%') {
                    inPercent = true;
                }
            } else if (!inFirstDigit) {
                if (ENCODED_CHARS.indexOf(c) != -1) {
                    inFirstDigit = true;
                    savedFirstDigit = c;
                } else {
                    inPercent = false;
                    result.append(c);
                }
            } else {
                if (ENCODED_CHARS.indexOf(c) != -1) {
                    result.append(Character.toLowerCase(savedFirstDigit));
                    result.append(Character.toLowerCase(c));
                } else {
                    result.append(savedFirstDigit);
                    result.append(c);
                }

                inPercent = false;
                inFirstDigit = false;
            }
        }

        if (inFirstDigit) {
            result.append(savedFirstDigit);
        }

        return result.toString();
    }

}
