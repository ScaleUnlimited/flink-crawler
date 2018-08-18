package com.scaleunlimited.flinkcrawler.tools;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.flinkcrawler.functions.TimerFunction;

public class TimerTool {
    static final Logger LOGGER = LoggerFactory.getLogger(TimerTool.class);
    
    static final String CHECKPOINTING_DIR = "file:///Users/schmed/Downloads/flink-crawler/checkpoints";
    
    public static void main(String[] args) {
        StreamExecutionEnvironment env;
        
        try {
            // Create an execution environment
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
            
            // Enable and configure checkpointing
            //
            // TODO Is this necessary for savepointing?
            //
            env.enableCheckpointing(5 * 60_000L, CheckpointingMode.AT_LEAST_ONCE);
//            StateBackend backend = new FsStateBackend(CHECKPOINTING_DIR);
//            env.setStateBackend(backend);
            
            // Process some elements (one every 10 seconds), which should also 
            // create a timer for one second from now.
            List<Integer> input = new ArrayList<Integer>();
            for (int i = 0; i < 60; i++) {
                input.add(i);
            }
            TimerTool tool = new TimerTool();
            SourceFunction<Integer> numbers =
                tool.new ParallelListSource<Integer>(input, 10_000L);

            // This operator doesn't really do much at all, but the first element
            // it processes will create a timer for (timestamp+1000).
            // Whenever that timer fires, it will create another timer for 
            // (timestamp+1000).
            TimerFunction timerFunction =
                new TimerFunction(new ArrayList<Long>(), 1000L);
            
            SingleOutputStreamOperator<Integer> processedNumbers = env
                .addSource(numbers)
                .returns(Integer.class)
                .keyBy(new TimerTool.IdentityKeySelector<Integer>())
                .process(timerFunction)
                .name("Processed numbers");
            
            // Write the numbers to standard output
            processedNumbers
                .print();
            
            // Execute that flow
            try {
                env.execute();
            } catch (Exception e) {
                LOGGER.error("Execution failed", e);
            }
            
        } catch (Exception e) {
            LOGGER.error("Configuration failed", e);
        }
    }

    @SuppressWarnings("serial")
    public
    static class IdentityKeySelector<T> implements KeySelector<T, T> {
    
        @Override
        public T getKey(T value) throws Exception {
            return value;
        }
    }

    // Note: I don't think I really need a parallel list source, but I just
    // stole this code from Ken's flink-streaming-kmeans and then added the
    // support for dribbling out the elements at msPerElement.
    @SuppressWarnings("serial")
    public class ParallelListSource<T> extends RichSourceFunction<T> {
        private List<T> _elements;
        private long _msPerElement;
        
        private transient int _parallelism;
        private transient int _subtaskIndex;
        private transient volatile boolean _running;
        
        public ParallelListSource(List<T> elements) {
            this(elements, 0L);
        }
        
        public ParallelListSource(List<T> elements, long msPerElement) {
            _elements = elements;
            _msPerElement = msPerElement;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
            _parallelism = context.getNumberOfParallelSubtasks();
            _subtaskIndex = context.getIndexOfThisSubtask();

        }
        
        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            _running = true;
            
            int index = 0;
            Iterator<T> iter = _elements.iterator();
            while (_running && iter.hasNext()) {
                if (_msPerElement > 0) {
                    Thread.sleep(_msPerElement);
                }
                T element = iter.next();
                if ((index % _parallelism) == _subtaskIndex) {
                    LOGGER.debug("Emitting {} at index {} for subtask {}", element.toString(), index, _subtaskIndex);
                    ctx.collect(element);
                }
                
                index += 1;
            }
        }

        @Override
        public void cancel() {
            _running = false;
        }

    }
}
