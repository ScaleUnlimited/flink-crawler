package com.scaleunlimited.flinkcrawler.metrics;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;



public class CounterUtils {

	private static final String DELIMITER = "->";  
	
	public static void increment(RuntimeContext runtimeContext, Enum<?> e) {
		increment(runtimeContext, enumToGroup(e), enumToCounter(e), 1);
	}

	public static void increment(RuntimeContext runtimeContext, Enum<?> e, long l) {
		increment(runtimeContext, enumToGroup(e), enumToCounter(e), l);
	}
	
	public static void increment(RuntimeContext runtimeContext, String group, String counter, long l) {
		if(runtimeContext != null) {
			LongCounter flinkCounter = getOrInitCounter(runtimeContext, mergeGroupCounter(group, counter));
			flinkCounter.add(l);
		}
	}

	public static long getCounterValue(RuntimeContext runtimeContext, Enum<?> e) {
		return getCounterValue(runtimeContext, enumToGroup(e), enumToCounter(e));
	}

	public static long getCounterValue(RuntimeContext runtimeContext, String group, String counter) {
		if(runtimeContext != null) {
			return getOrInitCounter(runtimeContext, mergeGroupCounter(group, counter)).getLocalValue();
		}
		else {
			return 0L;
		}
	}
	
	private static LongCounter getOrInitCounter(RuntimeContext runtimeContext, String counterName) {

		LongCounter lc = runtimeContext.getLongCounter(counterName);
		if (lc == null) {
			lc = new LongCounter();
			runtimeContext.addAccumulator(counterName, lc);
		}
		return lc;
	}
	
	public static String enumToGroup(Enum<?> e) {
		return e.getDeclaringClass().getSimpleName();  //  e.getDeclaringClass().getName();
	}

	/**
	 * Convert an Enum to the counter portion of its name.
	 *
	 * @param e
	 * @return
	 */
	public static String enumToCounter(Enum<?> e) {
		return e.name();
	}

	public static String mergeGroupCounter(String group, String counter) {
		return group + DELIMITER + counter;
	}

	public static boolean accInGroup(String group, String accKey) {
		return accKey.startsWith(group + DELIMITER);
	}

	public static boolean accMatchesGroupCounter(String accKey, String group, String counter) {
		return accKey.equals(group + DELIMITER + counter);
	}

	public static String groupCounterToGroup(String groupKey) {
		return groupKey.split(DELIMITER)[0];
	}

	public static String groupCounterToCounter(String groupKey) {
		return groupKey.split(DELIMITER)[1];
	}
}
