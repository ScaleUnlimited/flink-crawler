package com.scaleunlimited.flinkcrawler.functions;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.tools.TimerTool;

public class TimerTest {
    public static final Logger LOGGER = LoggerFactory.getLogger(TimerTest.class);

    private List<Long> _firedTimers = new ArrayList<Long>();

    @Before
    public void setUp() throws Exception {
    }
    
    @Test
    public void testTimerSaving() throws Throwable {
        
        // This operator doesn't really do much at all, but the first element
        // it processes will create a timer for (timestamp+1).
        // Whenever that timer fires, it will create another timer for 
        // (timestamp+1).
        KeyedProcessOperator<Integer, Integer, Integer> operator = 
            new KeyedProcessOperator<>(new TimerFunction(_firedTimers));
        
        // Create a test harness from scratch
        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness = 
            makeTestHarness(operator, null);
        
        // We begin at time zero
        testHarness.setProcessingTime(0);

        // Process some elements, which should also create a timer for time 1.
        int inputs[] = new int[] {1, 2, 3};
        for (int input : inputs) {
            testHarness.processElement(new StreamRecord<>(input));
        }
        
        // Force some time to pass, which should keep moving the timer ahead,
        // finally leaving it set for time 10.
        for (long i = 1; i < 10; i++) {
            testHarness.setProcessingTime(i);
        }
        
        // Save the state, which we assume should include the timer we set for
        // time 10.
        OperatorSubtaskState savedState = 
            testHarness.snapshot(0L, testHarness.getProcessingTime());
        
        // Close the first test harness
        testHarness.close();
        
        // Create a new test harness using the saved state (which we assume
        // includes the timer for time 10).
        testHarness = makeTestHarness(operator, savedState);
        
        // Force more time to pass, which should keep moving the timer ahead.
        for (long i = 10; i < 20; i++) {
            testHarness.setProcessingTime(i);
        }
        
        // Close the second test harness and make sure all the timers we expect
        // actually fired.
        testHarness.close();
        for (long i = 1; i < 20; i++) {
            
            // TODO This expectation currently fails, since Timers don't
            // seem to be included in the snapshot, at least the one produced by
            // the test harness.
            assertTrue(_firedTimers.contains(i));
        }
    }

    private OneInputStreamOperatorTestHarness<Integer, Integer> makeTestHarness(
            KeyedProcessOperator<Integer, Integer, Integer> operator,
            OperatorSubtaskState savedState) 
            throws Exception {
        OneInputStreamOperatorTestHarness<Integer, Integer> result;
        result = 
            new KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer>(
                    operator,
                    new TimerTool.IdentityKeySelector<Integer>(),
                    BasicTypeInfo.INT_TYPE_INFO);
        result.setup();
        if (savedState != null) {
            result.initializeState(savedState);
        }
        result.open();
        return result;
    }
}
