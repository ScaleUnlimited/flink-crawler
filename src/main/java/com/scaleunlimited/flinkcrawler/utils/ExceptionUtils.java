package com.scaleunlimited.flinkcrawler.utils;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;

import crawlercommons.fetcher.AbortedFetchException;
import crawlercommons.fetcher.AbortedFetchReason;
import crawlercommons.fetcher.IOFetchException;
import crawlercommons.fetcher.RedirectFetchException;
import crawlercommons.fetcher.RedirectFetchException.RedirectExceptionReason;
import crawlercommons.fetcher.UrlFetchException;

public class ExceptionUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionUtils.class);

    public static FetchStatus mapHttpStatusToFetchStatus(int httpStatus) {
        switch (httpStatus) {
            case HttpStatus.SC_OK:
                return FetchStatus.FETCHED;

            case HttpStatus.SC_FORBIDDEN:
                return FetchStatus.HTTP_FORBIDDEN;

            case HttpStatus.SC_UNAUTHORIZED:
            case HttpStatus.SC_PROXY_AUTHENTICATION_REQUIRED:
                return FetchStatus.HTTP_UNAUTHORIZED;

            case HttpStatus.SC_NOT_FOUND:
                return FetchStatus.HTTP_NOT_FOUND;

            case HttpStatus.SC_GONE:
                return FetchStatus.HTTP_GONE;

            case HttpStatus.SC_TEMPORARY_REDIRECT:
            case HttpStatus.SC_MOVED_TEMPORARILY:
            case HttpStatus.SC_SEE_OTHER:
                return FetchStatus.HTTP_TOO_MANY_REDIRECTS;

            case HttpStatus.SC_MOVED_PERMANENTLY:
                return FetchStatus.HTTP_MOVED_PERMANENTLY;

            default:
                if (httpStatus < 300) {
                    LOGGER.warn("Invalid HTTP status for exception: " + httpStatus);
                    return FetchStatus.HTTP_SERVER_ERROR;
                } else if (httpStatus < 400) {
                    return FetchStatus.HTTP_REDIRECTION_ERROR;
                } else if (httpStatus < 500) {
                    return FetchStatus.HTTP_CLIENT_ERROR;
                } else if (httpStatus < 600) {
                    return FetchStatus.HTTP_SERVER_ERROR;
                } else {
                    LOGGER.warn("Unknown status: " + httpStatus);
                    return FetchStatus.HTTP_SERVER_ERROR;
                }
        }
    }

    public static FetchStatus mapExceptionToFetchStatus(Exception e) {

        if (e instanceof AbortedFetchException) {
            AbortedFetchException afe = (AbortedFetchException) e;
            AbortedFetchReason abortReason = afe.getAbortReason();
            switch (abortReason) {
                case SLOW_RESPONSE_RATE:
                    return FetchStatus.ABORTED_SLOW_RESPONSE;

                case INVALID_MIMETYPE:
                case CONTENT_SIZE:
                    return FetchStatus.ABORTED_FETCH_SETTINGS;

                case INTERRUPTED:
                    return FetchStatus.SKIPPED_INTERRUPTED;

                default:
                    throw new RuntimeException("Unknown abort reason: " + abortReason);
            }

        } else if (e instanceof IOFetchException) {
            return FetchStatus.ERROR_IOEXCEPTION;
        } else if (e instanceof RedirectFetchException) {
            RedirectFetchException rfe = (RedirectFetchException) e;
            RedirectExceptionReason reason = rfe.getReason();
            switch (reason) {
                case TOO_MANY_REDIRECTS:
                    return FetchStatus.HTTP_TOO_MANY_REDIRECTS;
                case TEMP_REDIRECT_DISALLOWED:
                    return FetchStatus.HTTP_REDIRECTION_ERROR;
                case PERM_REDIRECT_DISALLOWED:
                    return FetchStatus.HTTP_MOVED_PERMANENTLY;
                default:
                    throw new RuntimeException("Unknown redirection exception reason: " + reason);
            }
        } else if (e instanceof UrlFetchException) {
            return FetchStatus.ERROR_INVALID_URL;
        }

        LOGGER.warn("Unknown exception: " + e.getMessage());
        return FetchStatus.HTTP_SERVER_ERROR;

    }

}
