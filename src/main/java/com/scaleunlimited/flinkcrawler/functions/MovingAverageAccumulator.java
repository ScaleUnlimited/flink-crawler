package com.scaleunlimited.flinkcrawler.functions;

import java.util.LinkedList;
import java.util.Queue;

public class MovingAverageAccumulator {

    private int _numEntries;
    private Queue<Float> _scores;
    
    public MovingAverageAccumulator(int numEntries) {
        _numEntries = numEntries;
        _scores = new LinkedList<>();
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
        if (_scores.isEmpty()) {
            return 0.0f;
        }
        
        float total = 0.0f;
        for (float score : _scores) {
            total += score;
        }
        
        return total / _scores.size();
    }
}
