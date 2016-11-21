package com.scaleunlimited.flinkcrawler.webgraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

public class SimpleWebGraphTest {

	@Test
	public void test() {
		String[] graph = new String[] {
				"",
				"# This is a top-level domain with two outlinks",
				"domain1.com\tdomain1.com/page1\tdomain1.com/page2",
				"# This is a page with no outlinks",
				"domain1.com/page1\t",
				"# This page has three outlinks. One doesn't exist, one is a loop back to the top, and one is a loop back to a sub-page",
				"domain1.com/page2\tdomain2.com\tdomain1.com\tdomain1.com/page1"
		};
		
		BaseWebGraph webGraph = new SimpleWebGraph(Arrays.asList(graph));
		assertNull(webGraph.getChildren("bogus"));
		
		Iterator<String> children = webGraph.getChildren("domain1.com");
		assertNotNull(children);
		assertEquals("domain1.com/page1", children.next());
		assertEquals("domain1.com/page2", children.next());
		assertFalse(children.hasNext());
		
		children = webGraph.getChildren("domain1.com/page1");
		assertNotNull(children);
		assertFalse(children.hasNext());

		children = webGraph.getChildren("domain1.com/page2");
		assertNotNull(children);
		assertEquals("domain2.com", children.next());
		assertEquals("domain1.com", children.next());
		assertEquals("domain1.com/page1", children.next());
	}

}
