package com.scaleunlimited.flinkcrawler.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import com.scaleunlimited.flinkcrawler.pojos.DomainScore;

@SuppressWarnings("serial")
public class MovingAverageFunction extends BaseMapFunction<DomainScore, DomainScore> implements CheckpointedFunction {

    private int _numEntries = 10;
    
    private transient ValueState<MovingAverageAccumulator> _state;
    
    public MovingAverageFunction(int numEntries) {
        super();
        
        _numEntries = numEntries;
    }
    
    @Override
    public DomainScore map(DomainScore score) throws Exception {
        MovingAverageAccumulator acc = _state.value();
        if (acc == null) {
            acc = new MovingAverageAccumulator(_numEntries);
        }
        
        acc.add(score.getScore());
        _state.update(acc);
        
        return new DomainScore(score.getPld(), acc.getResult());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ValueStateDescriptor<MovingAverageAccumulator> descriptor = new ValueStateDescriptor<>(
                "moving-average", TypeInformation.of(new TypeHint<MovingAverageAccumulator>() {
                }));
        _state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Nothing to do here, since we're using managed state.
    }

}
