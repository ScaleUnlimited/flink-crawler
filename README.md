# flink-crawler
A continuous scalable web crawler built on top of [Flink](http://flink.apache.org) and [crawler-commons](https://github.com/crawler-commons/crawler-commons), with bits of code borrowed from [bixo](https://github.com/bixo/bixo/).

The primary goals of flink-crawler are:

* Continuous, meaning pages are always being fetched. This avoids the inefficiencies of a batch-oriented crawler such as Bixo or Nutch, where the time spent processing the "crawl frontier" (aka CrawlDB) in each loop grows to where it winds up dominating the total time.
* Scalable, meaning the crawler should work for small crawls of a 100K pages up to big crawls which fetch billions of pages and track 100B+ links.
* Focused, meaning the crawler can be tuned to focus on pages and domains with the highest value, thus improving the efficiency of the crawl.
* Simple, meaning operationally it should be easy to set up and run a crawl, without requiring additional infrastructure beyond what's needed for Flink.

See the [Key Design Decisions](https://github.com/ScaleUnlimited/flink-crawler/wiki/Key-Design-Decisions) page for more details.
