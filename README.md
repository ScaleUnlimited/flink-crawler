# flink-crawler
A continuous scalable web crawler built on top of [Flink](http://flink.apache.org) and [crawler-commons](https://github.com/crawler-commons/crawler-commons), with bits of code borrowed from [bixo](https://github.com/bixo/bixo/).

# Key Design Decisions

## Crawl State

In Nutch, Bixo, and other Hadoop-based web crawlers the most popular way to maintain crawl state is in a sequence file. The start of each processing "loop" is a job that analyzes the this "crawl DB" to extract a set of URLs to be fetched, based on the fetch state, number of URLs per domain, target fetch duration, time since last fetch, summed scores of links pointing at a page, etc. As part of this processing you also want to merge in any URLs extracted from contect that was fetched/parsed in the previous crawl loop, as well as merging in status updates for URLs that were fetched.

Unfortunately this adds significant latency to the crawl, especially as the crawl DB grows in size. E.g. for a 500M page crawl, the crawl DB can wind up with 30B known URLs (so 60x more "known" URLs than what have been fetched). And a typical crawl loop quickly runs out of URLs for most domains, leaving a handful of domains that slowly are fetched (due to politeness limitations).

So for a continuous crawl, we need to maintain this crawl state such that we can:

1. Add newly discovered URLs that haven't been seen before.
1. Update the state of URLs that we've tried to fetch (result of request, time for next fetch, etc).
1. Update the score of (unfetched only?) URLs based on scoring of pages that link to this page.
1. Query to find the most interesting URLs to fetch for a given domain, given all of the above factors.
1. Work well with Flink's [checkpoint](https://ci.apache.org/projects/flink/flink-docs-master/internals/stream_checkpointing.html) support.

The above solution needs to efficiently handle > 1B URLs/server, to scale to 10B+ on a 10 server cluster.

I'm assuming we can quickly filter URLs based on per-domain politeness settings, separate from what we extracted from the crawl state (though we might want to send back information to the crawl state source about how much was filtered, as a way of avoiding sending too many URLs down the pipe).

It's possible we could use Kafka to maintain "state" in a way that scales, but it would be nice to avoid adding another thick technology slice to the stack.

We could save state using Elasticsearch.

We could craft our own "spill-to-disk" solution, as the set of active URLs (top URLs that should be fetched soon, for unique domains) is often pretty small. The bulk of URLs wind up being endless product links on big eCommerce sites, or spammy link farm URLs, etc. So having a "hot set" in memory that we can query, and a larger set on disk that occasionally gets scanned, is one option. There was a single-server crawler project called [IRLBot](http://irl.cs.tamu.edu/crawler/) that did something like this...we should read [this paper](http://irl.cs.tamu.edu/people/hsin-tsang/papers/www2008.pdf) on details.

### IRLBot-style approach

A key piece of IRLBot is something called DRUM, which uses an in-memory array to store <key, value, payload> data until it's full. Then this is sorted/merged against the backing disk file, and keys that already exist are either dropped (check only) or merged (check-update), and the new disk file is written out. During this process new keys can be forwarded for additional processing.

Typically the key is a hash, the value is any data that's needed for an update, and the payload is something that can be written to disk separate from the `<key, value>`. This means it can also be on disk when the `<key, value>` is in the in-memory queue, thus fitting more in memory.

As an example for processing "new" (added/discovered) URLs, the key is an 8-byte hash of the URL, the value is null, and the payload is the actual URL. This is what IRLBot calls the "URLseen" DRUM, and is used to filter out already seen URLs. It also forwards new URLs to the crawling pipeline.

A second important piece is how they handle budgeting crawl capacity. They calculate a budget for each PLD, based on how many other PLDs have refs to this PLD. The theory is that link farms aren't going to be able to pay for a gazillion PLDs to mess with this metric. Once they have a budget, then when the get a link that is new, they use a set of queues to organize these into groups, where each group size is equal to the budget. The description of how exactly IRLBot uses these queues is a bit confusing, but basically it keeps re-prioritizing URLs by re-partitioning them between the available queues. Though keeping a queue set per PLD doesn't seem scalable.

If we have the concept of a URL score, and a "budget" (percentage of capacity to give to that PLD, based on cluster crawl capacity and all known domains) then we could potentially use this approach to avoid spending much time processing URLs that we'll never want to crawl. Occasionally we'd need to re-process all queues, as the scores for URLs will change based on time (e.g. based on time since last crawl, for the recrawl case).

### Proposal for how to use DRUM approach

TODO - define a term for the key/value and payload files - a drum set? And that's either in-memory (key/value only, payload is alwas on disk) or completely on disk.

We have a crawl DB that maintains an in-memory list of URLs to be updated in the crawl DB. The list is actually three arrays, a long hash value, a ref to an optional "value", and an int offset into a separate payload data file.

When adding a new URL, we check to see if the hash already exists. If it does, then we have to merge the new URL to the existing URL. This potentially means creating a new (merged) payload, which is then written to the end of the payload file, and the payload offset is updated. If it doesn't exist, then we add it to the end of the list.

These URLs added to the end of the list are unsorted, so to check for whether the entry already exists, we use a binary sort over the inital (sorted) set of entries, and then a linear scan over the remaining. When the number of unsorted entries gets too long, we re-sort the list.

When we run out of space in this in-memory table, or there is some other external trigger (we need more URLs to fetch), then this in-memory list is merged with the existing (sorted) key/value file and payload file. The key/value file is read one chunk at a time into a separate buffer, and merged with the in-memory table. The results are written out to a new key/value file. When a payload is added or updated, it is appended to the existing payload file. Note this creates some unused data in the file, which is reclaimed during the compaction phase (see below).

While this merge is going on, two additional processing steps are happening. First, we have an in-memory priority queue of URLs to be fetched. We add entries based on a calculated URL score, which depends on many factors (URL status, estimated score, last fetched time, number of URLs for the host vs. the host "rank" (TBD), etc).

The other processing step is that we're archiving URLs that we are unlikely to want to fetch (score is too low). Instead of writing these to what will become the new key/value (and payload) files, we write them to a new archived key/value and payload file set. So each merge that we do will (potentially) generate a new archived file set (with a timestamped name?). This prevents us from having a very large set of entries in the primary file set that we keep re-processing on every merge but will never wind up fetching.

We might wind up adding a new URL to the primary file set that already exists in one of the archived file sets, but it's likely to have a very low score, so that's OK.

Eventually we'll have many (say 100) archived file sets. When that happens, we'll trigger a compaction, where we do a merge across the primary and all archived file sets, building the new primary and the new (single) archived file set. At this point we'll also create a new payload file for the primary file set, thus reclaiming any unused space.

Note that whenever we're doing a merge (or compaction), we set up a new in-memory key/value and payload file, so that data can continue flowing in while we're merging. The only time we'd be locked up is if the new in-memory list fills up before we finish, which should only happen during a compaction.

TBD: optimization to avoid putting a URL on the fetch list if we're already fetching it...When we're done with the merge, we also add all of these "to be fetched" URLs to the new in-memory queue, so that if a new URL is discovered that

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
