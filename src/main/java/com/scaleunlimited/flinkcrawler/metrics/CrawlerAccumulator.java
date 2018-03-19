package com.scaleunlimited.flinkcrawler.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;

public class CrawlerAccumulator {

    private RuntimeContext _runtimeContext;

    public CrawlerAccumulator(RuntimeContext runtimeContext) {
        _runtimeContext = runtimeContext;
    }

    /**
     * Increment the counter for the enum e by 1
     * 
     * @param e
     *            The enum to use as a name
     */
    public void increment(Enum<?> e) {
        CounterUtils.increment(_runtimeContext, e);
    }

    /**
     * Modify the counter for the enum e by changeBy - if positive the counter is incremented; if negative it is
     * decremented.
     * 
     * @param e
     *            The enum to use as a name
     * @param changeBy
     *            the value to modify the counter by
     */
    public void increment(Enum<?> e, long changeBy) {
        CounterUtils.increment(_runtimeContext, e, changeBy);
    }

    /**
     * Modify the counter by changeBy - if positive the counter is incremented; if negative it is decremented.
     * 
     * @param group
     *            The name of the group the counter belongs to
     * @param counter
     *            The name of the counter
     * @param changeBy
     *            the value to modify the counter by
     */
    public void increment(String group, String counter, long changeBy) {
        CounterUtils.increment(_runtimeContext, group, counter, changeBy);
    }

}
