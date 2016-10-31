package com.scaleunlimited.flinkcrawler.fetcher;

// Possible redirect handling modes. If a redirect is NOT followed because of this
// setting, then a RedirectFetchException is thrown, which is the same as what happens if
// too many redirects occur. But RedirectFetchException now has a reason field, which
// can be set to TOO_MANY_REDIRECTS, PERM_REDIRECT_DISALLOWED, or TEMP_REDIRECT_DISALLOWED.

public enum RedirectMode {
    
        FOLLOW_ALL,         // Fetcher will try to follow all redirects
        FOLLOW_TEMP,        // Temp redirects are automatically followed, but not pemanent.
        FOLLOW_NONE         // No redirects are followed.
}
