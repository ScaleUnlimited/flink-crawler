package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.pojos.DomainScore;

@SuppressWarnings("serial")
public class MovingAverageAggregator implements AggregateFunction<DomainScore, MovingAverageAccumulator<String>, DomainScore> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MovingAverageAggregator.class);

    private int _numEntries;
    
    public MovingAverageAggregator(int numEntries) {
        _numEntries = numEntries;
    }
    
    @Override
    public MovingAverageAccumulator<String> createAccumulator() {
        LOGGER.debug("Creating accumulator");
        
        return new MovingAverageAccumulator<String>(_numEntries);
    }

    @Override
    public MovingAverageAccumulator<String> add(DomainScore domainScore, MovingAverageAccumulator<String> acc) {
        if (acc.getKey() == null) {
            LOGGER.debug("Setting PLD to {}", domainScore.getPld());
            acc.setKey(domainScore.getPld());
        }
        
        LOGGER.debug("Adding {} for {}", domainScore.getScore(), acc.getKey());
        acc.add(domainScore.getScore());
        return acc;
    }

    @Override
    public DomainScore getResult(MovingAverageAccumulator<String> acc) {
        float average = acc.getResult();
        LOGGER.debug("Returning average {} for {}", average, acc.getKey());
        return new DomainScore(acc.getKey(), average);
    }

    @Override
    public MovingAverageAccumulator<String> merge(MovingAverageAccumulator<String> acc1,
            MovingAverageAccumulator<String> acc2) {
        return acc1.merge(acc2);
    }
    
}
