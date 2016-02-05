package com.winvector.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import com.winvector.db.DBUtil.DBHandle;
import com.winvector.util.BurstMap;
import com.winvector.util.RowCritique;

public final class TableControl {
	private static final String rowNumCol = "ORIGFILEROWNUMBER";
	private static final String fileNameCol = "ORIGFILENAME";
	private static final String insertTimeCol = "ORIGINSERTTIME";
	private static final String randCol = "ORIGRANDGROUP";
	private static Set<String> predefKeys = new LinkedHashSet<String>(Arrays.asList(new String[] {
			rowNumCol, fileNameCol, insertTimeCol, randCol
		}));
	private static final int fNameColNum = 1;

	private final String tableName;
	private final ArrayList<String> keys = new ArrayList<String>();
	private boolean[] isInt = null;
	private boolean[] isNumeric = null;
	private int[] sizes = null;

	private final String colQuote;
	
	private String createStatement = null;
	private String insertStatement = null;
	private String selectStatement = null;
	
	private int[] columnTypeCode = null;
	private String[] columnClassName = null;

	private final String SQLDOUBLETYPE;
	private final String SQLINTTYPE;
	private final String SQLTIMETYPE;
	
	public TableControl(final DBUtil.DBHandle dbHandle, final Properties props, final String tableName) {
		colQuote = dbHandle.colQuote;
		SQLDOUBLETYPE = props.getProperty("SQLDOUBLE", "DOUBLE PRECISION");
		SQLINTTYPE = props.getProperty("SQLINT", "BIGINT"); // Oracle uses NUMBER
		SQLTIMETYPE = props.getProperty("SQLTIME", "TIMESTAMP");
		this.tableName = tableName;
	}
	
	public static boolean couldBeDouble(final String v) {
		if(null==v) {
			return false;
		}
		final int len = v.length();
		if(len<=0) {
			return true;
		}
		if(len>40) {
			return false;
		}
		if(BurstMap.missingDoubleValue(v)) {
			return true;
		}
		if(v.equalsIgnoreCase(BurstMap.doublePosInfString)||v.equalsIgnoreCase(BurstMap.doubleNegInfString)) {
			return true;
		}
		try {
			Double.parseDouble(v);
			return true;
		} catch (NumberFormatException ex) {
		}
		return false;
	}

	public static boolean couldBeInt(final String v) {
		if(null==v) {
			return false;
		}
		final int len = v.length();
		if((len<0)||(len>20)) {
			return false;
		}
		try {
			Integer.parseInt(v);
			return true;
		} catch (NumberFormatException ex) {
		}
		return false;
	}
	
	private static class Ticker {
		private long reportWidth = 1000;
		private long reportTarget = 2*reportWidth;
		private long nDone = 0;
		private long lastReportTime = 0;
		private final long tickTarget = 30*1000L;
		
		public void tick() {
			nDone += 1;
			if(nDone>=reportTarget) {
				final Date now = new Date();
				System.out.println("\tprocessed " + nDone + "\t" + now);
				if((lastReportTime<=0)||((now.getTime()-lastReportTime)<tickTarget)) {
					reportWidth *= 2;
				}
				reportTarget += reportWidth;
				lastReportTime = now.getTime();
			}
		}
	}
	
	public void scanForDefs(final String fileName,
			final Iterable<BurstMap> source, final RowCritique gateKeeper) throws SQLException {
		// scan once to get field names and sizes and types
		final Ticker ticker = new Ticker();
		for(final BurstMap row: source) {
			if((gateKeeper==null)||(gateKeeper.accept(row))) {
				if(keys.isEmpty()) {
					for(final String pc: predefKeys) {
						keys.add(pc);
					}
					keys.addAll(row.keySet());
					sizes = new int[keys.size()];
					isInt = new boolean[keys.size()];
					isNumeric = new boolean[keys.size()];
					Arrays.fill(sizes,1);
					Arrays.fill(isInt,true);
					Arrays.fill(isNumeric,true);
				}
				int i = 0;
				for(final String k: keys) {
					if(!predefKeys.contains(k)) {
						String v = row.getAsString(k);
						if(v!=null) {
							v = v.trim();
							final int vlength = v.length();
							sizes[i] = Math.max(sizes[i],vlength+1);
							if((vlength>0)&&(!BurstMap.missingDoubleValue(v))) {
								if(isNumeric[i]) {
									if(!couldBeDouble(v)) {
										isNumeric[i] = false;
									}
								}
								if(isInt[i]) {
									if(!couldBeInt(v)) {
										isInt[i] = false;
									}
								}
							}
						}
					}
					++i;
				}
			}
			ticker.tick();
		}
		sizes[fNameColNum] = Math.max(sizes[fNameColNum],fileName.length()+1);
	}
	
	private static String stompMarks(final String s) {
		return java.text.Normalizer.normalize(s, java.text.Normalizer.Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}+",""); // from: http://stackoverflow.com/questions/285228/how-to-convert-utf-8-to-us-ascii-in-java
	}
	
	
	public static final Set<String> invalidColumnNames = new HashSet<String>();
	public static final String columnPrefix = "x";
	static {
		final String[] keywords = { // no longer quoting out columns, user will need to quote columns
		};
		for(final String kw: keywords) {
			invalidColumnNames.add(kw.toUpperCase());
		}
	}
	
	public static String plumpColumnName(final String kin, final Set<String> seen) {
		String k = kin.toUpperCase();
		final int colonIndex = k.indexOf(':');
		if(colonIndex>0) { 		// get rid of any trailing : type info
			k = k.substring(0,colonIndex);
		}		
		k = stompMarks(k).replaceAll("\\W+"," ").trim().replaceAll("\\s+","_");
		if((k.length()<=0)||invalidColumnNames.contains(k.toUpperCase())||(!Character.isLetter(k.charAt(0)))) {
			k = columnPrefix + k;
		}
		if(seen.contains(k.toUpperCase())) {
			int i = 2;
			while(true) {
				String kt = k + "_" + i;
				if(!seen.contains(kt.toUpperCase())) {
					k = kt;
					break;
				} else {
					++i;
				}
			}
		}
		seen.add(k.toUpperCase());
		return k;
	}
	
	
	public void buildSQLStatements() {
		final Set<String> seenColNames = new HashSet<String>();
		final StringBuilder createBuilder = new StringBuilder();
		createBuilder.append("CREATE TABLE " + tableName + " (");
		final StringBuilder insertBuilder = new StringBuilder();
		insertBuilder.append("INSERT INTO " + tableName + " (");
		final StringBuilder selectBuilder = new StringBuilder();
		selectBuilder.append("SELECT ");
		{
			int i = 0;
			for(final String k: keys) {
				if(i>0) {
					createBuilder.append(",");
					insertBuilder.append(",");
					selectBuilder.append(",");
				}
				final String colName = plumpColumnName(k,seenColNames);
				if(predefKeys.contains(k)) {
					if(rowNumCol.equalsIgnoreCase(k)||randCol.equalsIgnoreCase(k)) {
						createBuilder.append(" " + colQuote + colName  + colQuote + " " + SQLINTTYPE);
					} else if(fileNameCol.equalsIgnoreCase(k)) {
						createBuilder.append(" " + colQuote + colName + colQuote + " VARCHAR(" + sizes[i] + ")");
					} else if(insertTimeCol.equalsIgnoreCase(k)) {
						createBuilder.append(" " + colQuote + colName  + colQuote + " " + SQLTIMETYPE);
					}
				} else {
					if(isInt[i]) {
						createBuilder.append(" " + colQuote + colName  + colQuote + " " + SQLINTTYPE);
					} else if(isNumeric[i]) {
						createBuilder.append(" " + colQuote + colName + colQuote + " " + SQLDOUBLETYPE);
					} else {
						createBuilder.append(" " + colQuote + colName + colQuote + " VARCHAR(" + sizes[i] + ")");
					}
				}
				insertBuilder.append(" " + colQuote + colName + colQuote);
				selectBuilder.append(" " + colQuote + colName + colQuote);
				++i;
			}
		}
		createBuilder.append(" )");
		insertBuilder.append(" ) VALUES (");
		selectBuilder.append(" FROM " + tableName);
		for(int i=0;i<sizes.length;++i) {
			if(i>0) {
				insertBuilder.append(",");
			}
			insertBuilder.append(" ?");
		}
		insertBuilder.append(" )");			
		createStatement = createBuilder.toString();
		insertStatement = insertBuilder.toString();
		selectStatement = selectBuilder.toString();
	}
	
	public void createTable(final DBHandle handle) throws SQLException {
		// set up table
		final Statement stmt = handle.conn.createStatement();
		handle.conn.commit();
		try {
			final String dropStatement = "DROP TABLE " + tableName;
			System.out.println("\texecuting: " + dropStatement);
			stmt.executeUpdate(dropStatement);
			handle.conn.commit();
		} catch (Exception ex) {
			handle.conn.rollback();
		}
		System.out.println("\texecuting: " + createStatement);
		stmt.executeUpdate(createStatement);
		handle.conn.commit();
		// get type codes back
		final ResultSet rs = stmt.executeQuery(selectStatement);
		final ResultSetMetaData rsm = rs.getMetaData();
		columnTypeCode = new int[sizes.length];
		columnClassName = new String[sizes.length];
		for(int i=0;i<sizes.length;++i) {
			columnTypeCode[i] = rsm.getColumnType(i+1);
			columnClassName[i] = rsm.getColumnClassName(i+1);
		}
		rs.close();
		stmt.close();			
	}
	
	public long loadData(final String fileName, final Date insertTime, final Random rand,
			final Iterable<BurstMap> source, final RowCritique gateKeeper,
			final DBHandle handle) throws SQLException {
		// scan again and populate
		System.out.println("\texecuting: " + insertStatement);
		handle.conn.commit();
		final Timestamp insertTimeStamp = new Timestamp(insertTime.getTime());
		final PreparedStatement stmtA = handle.conn.prepareStatement(insertStatement);
		long nInserted = 0;
		final Ticker ticker = new Ticker();
		for(final BurstMap row: source) {
			if((gateKeeper==null)||(gateKeeper.accept(row))) {
				int i = 0;
				for(final String k: keys) {
					if(predefKeys.contains(k)) {
						if(rowNumCol.equalsIgnoreCase(k)) {
							stmtA.setLong(i+1,nInserted+1);
						} else if(fileNameCol.equalsIgnoreCase(k)) {
							stmtA.setString(i+1,fileName);
						} else if(insertTimeCol.equalsIgnoreCase(k)) {
							stmtA.setTimestamp(i+1,insertTimeStamp);
						} else if(randCol.equalsIgnoreCase(k)) {
							final int randv = rand.nextInt(1000);
							stmtA.setLong(i+1,randv);
						}
					} else {
						if(isInt[i]) {
							final Long asLong = row.getAsLong(k);
							if(asLong==null) {
								stmtA.setNull(i+1,columnTypeCode[i]);
							} else {
								stmtA.setLong(i+1,asLong);
							}
						} else if(isNumeric[i]) {	
							final double asDouble = row.getAsDouble(k);
							if(Double.isNaN(asDouble)) {
								stmtA.setNull(i+1,columnTypeCode[i]);
							} else {
								stmtA.setDouble(i+1,asDouble);
							}
						} else {
							final String asString = row.getAsString(k);
							if(asString==null) {
								stmtA.setNull(i+1,columnTypeCode[i]);
							} else {
								stmtA.setString(i+1,asString);
							}
						}
					}
					++i;
				}
				stmtA.executeUpdate();
				++nInserted;
				if(nInserted%1000==0) {
					handle.conn.commit();
				}
			}
			ticker.tick();
		}
		handle.conn.commit();
		stmtA.close();
		return nInserted;
	}
}
