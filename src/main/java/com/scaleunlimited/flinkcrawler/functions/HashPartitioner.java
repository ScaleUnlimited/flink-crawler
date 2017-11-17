package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.Partitioner;

@SuppressWarnings("serial")
public class HashPartitioner implements Partitioner<Integer> {

	@Override
	public int partition(Integer key, int numPartitions) {
		return Math.abs(key % numPartitions);
	}
}
