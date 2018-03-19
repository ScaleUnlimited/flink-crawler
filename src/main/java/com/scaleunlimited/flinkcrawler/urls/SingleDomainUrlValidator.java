package com.scaleunlimited.flinkcrawler.urls;

import java.net.MalformedURLException;
import java.net.URL;

import crawlercommons.domains.PaidLevelDomain;

@SuppressWarnings("serial")
public class SingleDomainUrlValidator extends SimpleUrlValidator {
    private String _singleDomain;

    public SingleDomainUrlValidator(String singleDomain) {
        super();
        _singleDomain = singleDomain;
    }

    @Override
    public boolean isValid(String urlString) {
        if (!(super.isValid(urlString))) {
            return false;
        }
        return isUrlWithinDomain(urlString, _singleDomain);
    }

    /**
     * Check whether the domain of the URL is the given domain or a subdomain of the given domain.
     * 
     * @param url
     * @param domain
     * @return true iff url is "within" domain
     */
    public static boolean isUrlWithinDomain(String url, String domain) {
        try {
            for (String urlDomain = new URL(url)
                    .getHost(); urlDomain != null; urlDomain = getSuperDomain(urlDomain)) {
                if (urlDomain.equalsIgnoreCase(domain)) {
                    return true;
                }
            }
        } catch (MalformedURLException e) {
            return false;
        }

        return false;
    }

    /**
     * Extract the domain immediately containing this subdomain.
     * 
     * @param hostname
     * @return immediate super domain of hostname, or null if hostname is already a paid-level domain (i.e., not really
     *         a subdomain).
     */
    public static String getSuperDomain(String hostname) {
        String pld = PaidLevelDomain.getPLD(hostname);
        if (hostname.equalsIgnoreCase(pld)) {
            return null;
        }
        return hostname.substring(hostname.indexOf(".") + 1);
    }

}