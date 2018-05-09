package com.scaleunlimited.flinkcrawler.focused;

import java.io.Serializable;

import org.apache.flink.api.common.functions.RuntimeContext;

import com.scaleunlimited.flinkcrawler.parser.ParserResult;

@SuppressWarnings("serial")
public abstract class BasePageScorer implements Serializable {

    public void open(RuntimeContext context) throws Exception {
    }

    public void close() throws Exception {
    }

    public abstract float score(ParserResult parse);

}
