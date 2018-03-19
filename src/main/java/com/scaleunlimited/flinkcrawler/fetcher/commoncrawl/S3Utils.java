package com.scaleunlimited.flinkcrawler.fetcher.commoncrawl;

public class S3Utils {

    // Path to files in S3 looks like s3://commoncrawl/cc-index/collections/CC-MAIN-2017-17/indexes/cluster.idx
    // "2017-17" is a crawl ID
    //
    // "cluster.idx" is the name of an index file (the single secondary index file, in this case)
    // If it's one of the many primary index files, the name is cdx-<ddddd>.gz, e.g. cdx-00000.gz
    private static final String INDEX_FILES_PATH = "cc-index/collections/CC-MAIN-%s/indexes/%s";

    private static final String COMMONCRAWL_BUCKET = "commoncrawl";

    public static String makeS3FilePath(String crawlId, String filename) {
        return String.format(INDEX_FILES_PATH, crawlId, filename);
    }

    public static String getBucket() {
        return COMMONCRAWL_BUCKET;
    }
}
