package com.winvector.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class TestHBurster {
	@Test
	public void testFix() {
		final String sep = "\\|";
		final String[] headerFlds = HBurster.buildHeaderFlds("a|a|b".split(sep));
		final String[] expect = { "a", "a_2", "b" };
		assertNotNull(headerFlds);
		assertEquals(expect.length,headerFlds.length);
		for(int i=0;i<expect.length;++i) {
			assertEquals(expect[i],headerFlds[i]);
		}
	}
}
