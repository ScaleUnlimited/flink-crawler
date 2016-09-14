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

### IRLBot-style approach

A key piece of IRLBot is something called DRUM, which uses an in-memory array to store <key, value, payload> data until it's full. Then this is sorted/merged against the backing disk file, and keys that already exist are either dropped (check only) or merged (check-update), and the new disk file is written out. During this process new keys can be forwarded for additional processing.

Typically the key is a hash, the value is any data that's needed for an update, and the payload is something that can be written to disk separate from the `<key, value>`. This means it can also be on disk when the `<key, value>` is in the in-memory queue, thus fitting more in memory.

As an example for processing "new" (added/discovered) URLs, the key is an 8-byte hash of the URL, the value is null, and the payload is the actual URL. This is what IRLBot calls the "URLseen" DRUM, and is used to filter out already seen URLs. It also forwards new URLs to the crawling pipeline.

A second important piece is how they handle budgeting crawl capacity. They calculate a budget for each PLD, based on how many other PLDs have refs to this PLD. The theory is that link farms aren't going to be able to pay for a gazillion PLDs to mess with this metric. Once they have a budget, then when the get a link that is new, they use a set of queues to organize these into groups, where each group size is equal to the budget. The description of how exactly IRLBot uses these queues is a bit confusing, but basically it keeps re-prioritizing URLs by re-partitioning them between the available queues. Though keeping a queue set per PLD doesn't seem scalable.

If we have the concept of a URL score, and a "budget" (percentage of capacity to give to that PLD, based on cluster crawl capacity and all known domains) then we could potentially use this approach to avoid spending much time processing URLs that we'll never want to crawl. Occasionally we'd need to re-process all queues, as the scores for URLs will change based on time (e.g. based on time since last crawl, for the recrawl case).

## URL Normalization

Whenever normalization changes, the crawl state potentially becomes invalid.

## Format for crawl results

Use WARC? Or Avro?

## Testing

We should resurrect the web graph support (was used by Bixo at one point), as well as a pluggable synthetic graph generator, and use both for integration testing that isn't going to need external web access.

There's a new [flink-spector](https://github.com/ottogroup/flink-spector) test library for Flink that looks interesting.

# Flink

## Documentation

https://ci.apache.org/projects/flink/flink-docs-release-1.1/apis/streaming/index.html

## Tutorials

http://data-artisans.com/robust-stream-processing-flink-walkthrough/

## Interesting talks

http://www.slideshare.net/tillrohrmann/fault-tolerance-and-job-recovery-in-apache-flink
