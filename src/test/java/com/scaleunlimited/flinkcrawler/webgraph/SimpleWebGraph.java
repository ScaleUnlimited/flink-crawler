package com.scaleunlimited.flinkcrawler.webgraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SimpleWebGraph extends BaseWebGraph {

	private Map<String, List<String>> _graph;
	
	/**
	 * Given a list of text lines with the web graph, convert this into a web graph data structure.
	 * 
	 * The format of lines in the file are:
	 *	<page URL><tab><outline><tab><outlink>...<return>
	 *
	 * Where each page can have 0..n outlinks. If a page has no outlinks, it just contains <page URL><tab>
	 * 
	 * Empty lines, or lines beginning with '#' are igored.
	 * 
	 * @param nodes
	 */
	public SimpleWebGraph(List<String> nodes) {
		_graph = new HashMap<>(nodes.size());
		
		for (String node : nodes) {
			node = node.trim();
			if (node.isEmpty() || node.startsWith("#")) {
				continue;
			}
			
			String pieces[] = node.split("\t");
			String parent = pieces[0];
			if (_graph.containsKey(parent)) {
				throw new IllegalArgumentException("Duplicate node name found: " + parent);
			}
			
			List<String> children = new ArrayList<>(pieces.length - 1);
			for (int i = 1; i < pieces.length; i++) {
				children.add(pieces[i]);
			}
			
			_graph.put(parent, children);
		}
	}
	
	@Override
	public Iterator<String> getChildren(String parent) {
		List<String> children = _graph.get(parent);
		if (children == null) {
			return null;
		} else {
			return children.iterator();
		}
	}

}
