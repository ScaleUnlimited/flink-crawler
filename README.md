# flink-crawler
A continuous scalable web crawler built on top of [Flink](http://flink.apache.org) and [crawler-commons](https://github.com/crawler-commons/crawler-commons), with bits of code borrowed from [bixo](https://github.com/bixo/bixo/).

# Key Design Decisions

## Crawl State

In Nutch, Bixo, and other Hadoop-based web crawlers the most popular way to maintain crawl state is in a sequence file. The start of each processing "loop" is a job that analyzes the this "crawl DB" to extract a set of URLs to be fetched, based on the fetch state, number of URLs per domain, target fetch duration, time since last fetch, summed scores of links pointing at a page, etc.

Unfortunately this adds significant latency to the crawl, especially as the crawl DB grows in size. E.g. for a 500M page crawl, the crawl DB can wind up with 30B known URLs. And a typical crawl loop quickly runs out of URLs for most domains, leaving a handful of domains that slowly are fetched (due to politeness limitations).

So for a continuous crawl, we need to maintain state such that we can continuously query to find the most interesting URLs to fetch, given all of the above factors (including politeness). This state needs to handle > 1B URLs/server, to scale to 10B+ on a 10 server cluster. And the state needs to work well with Flink's [checkpoint](https://ci.apache.org/projects/flink/flink-docs-master/internals/stream_checkpointing.html) support.

It's possible we could use Kafka to maintain "state" in a way that scales, but it would be nice to avoid adding another thick technology slice to the stack.

We could save state using Elasticsearch.

We could craft our own "spill-to-disk" solution, as the set of active URLs (top URLs that should be fetched soon, for unique domains) is often pretty small. The bulk of URLs wind up being endless product links on big eCommerce sites, or spammy link farm URLs, etc. So having a "hot set" in memory that we can query, and a larger set on disk that occasionally gets scanned, is one option. There was a single-server crawler project called [IRLBot](http://irl.cs.tamu.edu/crawler/) that did something like this...we should read [this paper](http://irl.cs.tamu.edu/people/hsin-tsang/papers/www2008.pdf) on details.

## URL Normalization

Whenever normalization changes, the crawl state potentially becomes invalid.

## Format for crawl results

Use WARC? Or Avro?

## Testing

We should resurrect the web graph support (was used by Bixo at one point), as well as a pluggable synthetic graph generator, and use both for integration testing that isn't going to need external web access.



