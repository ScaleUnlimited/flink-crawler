/*
 * Copyright 2009-2015 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.scaleunlimited.flinkcrawler.fetcher;

import org.apache.http.HttpStatus;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.FetchStatus;

@SuppressWarnings({ "serial" })
public class HttpFetchException extends BaseFetchException {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpFetchException.class);
    
    private int _httpStatus;
    private Metadata _httpHeaders;
    
    public HttpFetchException() {
        super();
    }
    
    public HttpFetchException(String url, String msg, int httpStatus, Metadata httpHeaders) {
        super(url, buildMessage(msg, httpStatus, httpHeaders));
        _httpStatus = httpStatus;
        _httpHeaders = httpHeaders;
    }
    
    public int getHttpStatus() {
        return _httpStatus;
    }
    
    public Metadata getHttpHeaders() {
        return _httpHeaders;
    }

    private static String buildMessage(String msg, int httpStatus, Metadata httpHeaders) {
        StringBuilder result = new StringBuilder(msg);
        result.append(" (");
        result.append(httpStatus);
        result.append(")");
        
        String headers = httpHeaders.toString();
        if (headers.length() > 0) {
            result.append(" Headers: ");
            result.append(headers);
        }
        
        return result.toString();
    }
    
    @Override
    public FetchStatus mapToUrlStatus() {
        switch (_httpStatus) {
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
            if (_httpStatus < 300) {
                LOGGER.warn("Invalid HTTP status for exception: " + _httpStatus);
                return FetchStatus.HTTP_SERVER_ERROR;
            } else if (_httpStatus < 400) {
                return FetchStatus.HTTP_REDIRECTION_ERROR;
            } else if (_httpStatus < 500) {
                return FetchStatus.HTTP_CLIENT_ERROR;
            } else if (_httpStatus < 600) {
                return FetchStatus.HTTP_SERVER_ERROR;
            } else {
                LOGGER.warn("Unknown HTTP status for exception: " + _httpStatus);
                return FetchStatus.HTTP_SERVER_ERROR;
            }
        }
    }

}
