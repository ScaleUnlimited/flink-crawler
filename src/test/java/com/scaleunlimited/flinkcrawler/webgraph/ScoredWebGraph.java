package com.scaleunlimited.flinkcrawler.webgraph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.scaleunlimited.flinkcrawler.urls.BaseUrlNormalizer;

@SuppressWarnings("serial")
public class ScoredWebGraph extends SimpleWebGraph {

	private Map<String, Float> _scores;

	public ScoredWebGraph(BaseUrlNormalizer normalizer) {
		super(normalizer);
		
		_scores = new HashMap<>();
	}

	public ScoredWebGraph(BaseUrlNormalizer normalizer, List<String> nodes) {
		super(normalizer, nodes);
		
		_scores = new HashMap<>();
	}
	
	public ScoredWebGraph add(String parent, float score, String... children) {
		parent = normalize(parent);
		
		if (_scores.containsKey(parent)) {
			throw new IllegalArgumentException("Duplicate node name found: " + parent);
		}

		_scores.put(parent, score);
		
		add(parent, children);
		return this;
	}
	
	@Override
	public float getScore(String urlToFetch) {
		Float score = _scores.get(urlToFetch);
		if (score == null) {
			return 0.0f;
		} else {
			return score;
		}
	}

}
