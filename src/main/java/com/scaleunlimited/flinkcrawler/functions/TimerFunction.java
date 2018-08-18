package com.scaleunlimited.flinkcrawler.functions;

import java.util.List;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class TimerFunction
    extends KeyedProcessFunction<Integer, Integer, Integer> {
    static final Logger LOGGER = LoggerFactory.getLogger(TimerFunction.class);
   
    List<Long> _firedTimers;
    long _period;

    public TimerFunction(List<Long> firedTimers) {
        this(firedTimers, 1);
    }

    public TimerFunction(List<Long> firedTimers, long period) {
        super();
        _firedTimers = firedTimers;
        _period = period;
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<Integer, Integer, Integer>.OnTimerContext context,
                        Collector<Integer> out) throws Exception {
        super.onTimer(timestamp, context, out);
        _firedTimers.add(timestamp);
        long nextTimestamp = timestamp + _period;
        LOGGER.info("Firing at {}; Setting new timer for {}",
                    timestamp,
                    nextTimestamp);
        context.timerService().registerProcessingTimeTimer(nextTimestamp);
    }

    @Override
    public void processElement( Integer input,
                                KeyedProcessFunction<Integer, Integer, Integer>.Context context,
                                Collector<Integer> out)
        throws Exception {
        
        LOGGER.info("Processing input {}", input);
        if (_firedTimers.isEmpty()) {
            long firstTimestamp = 
                context.timerService().currentProcessingTime() + _period;
            LOGGER.info("Setting initial timer for {}",
                        firstTimestamp);
            context.timerService().registerProcessingTimeTimer(firstTimestamp);
        }
        
        out.collect(input);
    }
}