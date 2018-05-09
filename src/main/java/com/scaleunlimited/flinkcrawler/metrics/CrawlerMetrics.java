package com.scaleunlimited.flinkcrawler.metrics;

public enum CrawlerMetrics {

    GAUGE_URLS_CURRENTLY_BEING_FETCHED("URLsCurrentlyBeingFetched"), 
    GAUGE_URLS_FETCHED_PER_SECOND("URLsFetchedPerSeconds"),
    GAUGE_URLS_IN_FETCH_QUEUE("URLsInFetchQueue"),
    GAUGE_URLS_IN_FLIGHT("URLsInFlight"),
    GAUGE_UNIQUE_PLDS("UniquePLDs"),
    COUNTER_PAGES_PARSED("PagesParsed"),
    COUNTER_PAGES_NOTPARSED("PagesFailedParse");

    private String _name;

    CrawlerMetrics(String name) {
        _name = name;
    }

    @Override
    public String toString() {
        return _name;
    }

}
