package com.scaleunlimited.flinkcrawler.focused;

import com.scaleunlimited.flinkcrawler.parser.ParserResult;

@SuppressWarnings("serial")
public class AllEqualPageScorer extends BasePageScorer {

    @Override
    public float score(ParserResult parse) {
        return 1.0f;
    }

}
