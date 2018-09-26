package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import com.scaleunlimited.flinkcrawler.pojos.BaseUrl;
import com.scaleunlimited.flinkcrawler.utils.UrlLogger;

@SuppressWarnings("serial")
public abstract class BaseProcessFunction<IN, OUT> extends ProcessFunction<IN, OUT> {

    protected transient int _parallelism;
    protected transient int _partition;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        RuntimeContext context = getRuntimeContext();
        _parallelism = context.getNumberOfParallelSubtasks();
        _partition = context.getIndexOfThisSubtask() + 1;
    }

    protected void record(Class<?> clazz, BaseUrl url, String... metaData) {
        UrlLogger.record(clazz, _partition, _parallelism, url, metaData);
    }

    protected void record(Class<?> clazz, BaseUrl url) {
        UrlLogger.record(clazz, _partition, _parallelism, url);
    }

}
