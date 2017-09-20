package com.scaleunlimited.flinkcrawler.focused;

import java.io.Serializable;

import com.scaleunlimited.flinkcrawler.parser.ParserResult;

@SuppressWarnings("serial")
public abstract class BasePageScorer implements Serializable {

	public BasePageScorer() {
	}

	public abstract float score(ParserResult parse);

}
