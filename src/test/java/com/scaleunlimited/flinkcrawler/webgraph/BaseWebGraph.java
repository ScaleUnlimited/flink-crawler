package com.scaleunlimited.flinkcrawler.webgraph;

import java.util.Iterator;

public abstract class BaseWebGraph {
	/**
	 * Return an iterator that emits node names for all children of <parent>.
	 * 
	 * @param parent Name of parent node
	 * @return iterator over children, or null if <parent> doesn't exist.
	 */
	public abstract Iterator<String> getChildren(String parent);
	
}
