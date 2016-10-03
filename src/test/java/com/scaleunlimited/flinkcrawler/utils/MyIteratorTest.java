package com.scaleunlimited.flinkcrawler.utils;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MyIteratorTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void getNextUrlStringTest() {
		assertEquals(	"http://www.transpac.com/page-2",
						MyIterator.getNextUrlString("http://www.transpac.com/page-1"));
	}

}
