package com.winvector.dm;

import static org.junit.Assert.*;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.junit.Test;

import com.winvector.db.DBIterable;
import com.winvector.db.DBUtil;
import com.winvector.db.DBUtil.DBHandle;
import com.winvector.util.BurstMap;

public class TestDBIt {
	
	/**
	 * remove a directory containing only ordinary files (no directories)
	 * @param d
	 */
	private static boolean rmDir(final File d) {
		final File[] listFiles = d.listFiles();
		for(final File f : listFiles) {
			final String name = f.getName();
			if((name.compareTo(".")!=0)&&(name.compareTo("..")!=0)) {
				if(f.isDirectory()) {
					return false;
				}
			}
		}
		for(final File f : listFiles) {
			final String name = f.getName();
			if((name.compareTo(".")!=0)&&(name.compareTo("..")!=0)) {
				f.delete();
			}
		}
		d.delete();
		return true;
	}
	
	@Test
	public void testDBit() throws Exception {
		final File dir = File.createTempFile("SQLScrewdriverJUnitTest_",".db");
		dir.delete();
		dir.mkdir();
		final String dbName = new File(dir,"H2DB").getAbsolutePath();
		final String comment = "JunitTestDB";
		final String dbUserName = "JunitTestDB";
		final String dbPassword = "JunitTestDB";
		final String dbURL = "jdbc:h2:" + dbName;
		final String driver = "org.h2.Driver";
		final boolean readOnly = false;
		final DBHandle dbhandle = DBUtil.buildConnection(comment, dbUserName, dbPassword, dbURL, driver, readOnly);
		Exception ex = null;
		try {
			final Statement stmt = dbhandle.conn.createStatement();
			stmt.executeUpdate("CREATE TABLE TESTTAB (IARG INT, SARG VARCHAR(128), BIARG NUMERIC(30), BNARG NUMERIC(30,20), DARG DOUBLE PRECISION)");
			final PreparedStatement pstmt = dbhandle.conn.prepareStatement("INSERT INTO TESTTAB (IARG, SARG, BIARG, BNARG, DARG) VALUES (?, ?, ?, ?, ?)");
			pstmt.setInt(1,7);
			pstmt.setString(2,"hi there");
			pstmt.setBigDecimal(3,new BigDecimal(BigInteger.TEN));
			pstmt.setBigDecimal(4,new BigDecimal(new BigInteger("123"),2));
			pstmt.setDouble(5,1.7);
			pstmt.executeUpdate();
			pstmt.close();
			final DBIterable it = new DBIterable(stmt,"SELECT * FROM TESTTAB");
			long n = 0;
			for(final BurstMap row: it) {
				final long vIARG = row.getAsLong("IARG");
				final String vSARG = row.getAsString("SARG");
				final long vBIARG = row.getAsLong("BIARG");
				final double vBNARG = row.getAsDouble("BNARG");
				final double vDARG = row.getAsDouble("DARG");
				assertEquals(7,vIARG);
				assertEquals("hi there",vSARG);
				assertEquals(10,vBIARG);
				assertTrue(Math.abs(1.23-vBNARG)<1.0e-10);
				assertTrue(Math.abs(1.7-vDARG)<1.0e-10);
				++n;
			}
			assertEquals(1,n);
			stmt.close();
		} catch (Exception e) {
			ex = e;
		} finally {
			dbhandle.conn.close();
			rmDir(dir);
		}
		if(null!=ex) {
			throw ex;
		}
	}
	
}
