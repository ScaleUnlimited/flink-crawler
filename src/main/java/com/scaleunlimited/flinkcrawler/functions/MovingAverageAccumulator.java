package com.scaleunlimited.flinkcrawler.functions;

import java.util.LinkedList;
import java.util.Queue;

public class MovingAverageAccumulator<T> {

    private int _numEntries;
    private Queue<Float> _scores;
    private T _key;
    
    public MovingAverageAccumulator(int numEntries) {
        _numEntries = numEntries;
        _scores = new LinkedList<>();
        _key = null;
    }

    public T getKey() {
        return _key;
    }
    
    public void setKey(T key) {
        _key = key;
    }
    
    public Queue<Float> getScores() {
        return _scores;
    }
    
    public void add(float domainScore) {
        _scores.add(domainScore);
        while (_scores.size() > _numEntries) {
            _scores.remove();
        }
    }

    public float getResult() {
        float total = 0.0f;
        for (float score : _scores) {
            total += score;
        }
        
        return total / _scores.size();
    }

    public MovingAverageAccumulator<T> merge(MovingAverageAccumulator<T> acc2) {
        for (Float score : acc2.getScores()) {
            add(score);
        }
        
        return this;
    }
}
