package com.scaleunlimited.flinkcrawler.webgraph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public class SimpleWebGraph extends BaseWebGraph {

	private Map<String, List<String>> _graph;
	
	public SimpleWebGraph() {
		_graph = new HashMap<>();
	}
	
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
			String[] children = Arrays.copyOfRange(pieces, 1, pieces.length);
			add(parent, children);
		}
	}
	
	public SimpleWebGraph add(String parent, String... children) {
		if (_graph.containsKey(parent)) {
			throw new IllegalArgumentException("Duplicate node name found: " + parent);
		}

		_graph.put(parent, Arrays.asList(children));
		return this;
	}
	
	@Override
	public Iterator<String> getChildren(String parent) {
		// TODO we have a webgraph with entries that don't have a protocol. So if we can't
		// find an entry for parent, and it starts with http, then strip that off and
		// try again.
		
		// Hmm, we also have the issue of domain.com/ vs. domain.com, since our
		// normalizer adds that. Might want to put normalized entries in the the graph.
		List<String> children = _graph.get(parent);
		if ((children == null) && parent.startsWith("http")) {
			children = _graph.get(parent.replaceFirst("(http|https)://", ""));
		}
		
		if (children == null) {
			return null;
		} else {
			return children.iterator();
		}
	}

}
