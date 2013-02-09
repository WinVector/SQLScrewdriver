package com.winvector.db;

import java.io.File;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import com.winvector.db.DBUtil.DBHandle;
import com.winvector.util.BurstMap;
import com.winvector.util.RowCritique;
import com.winvector.util.TrivialReader;


public class LoadTable {
	private final static String colQuote = "\"";
	
	public static void main(final String[] args) throws Exception {
		final URI propsURI = new URI(args[0]);
		final char sep = args[1].charAt(0);
		final URI inURI = new URI(args[2]);
		final String tableName = args[3];
		
		System.out.println("start LoadTable\t" + new Date());
		System.out.println("\tcwd: " + (new File(".")).getAbsolutePath());
		System.out.println("\tDBProperties XML:\t" + propsURI.toString());
		System.out.println("\tsep: " + sep);
		System.out.println("\tSource URI:\t" + inURI);
		System.out.println("\ttableName:\t" + tableName);
		final DBHandle handle = DBUtil.buildConnection(propsURI,false);
		System.out.println("\tdb:\t" + handle);
		
		final Iterable<BurstMap> source = new TrivialReader(inURI,sep,null,true,null, false);
		loadTable(source, null, tableName, handle);
		handle.conn.close();
		
		System.out.println("done LoadTable\t" + new Date());
	}

	public static final Set<String> invalidColumnNames = new HashSet<String>();
	public static final String columnPrefix = "x";
	static {
		final String[] keywords = { // no longer quoting out columns, user will need to quote columns
		};
		for(final String kw: keywords) {
			invalidColumnNames.add(kw.toLowerCase());
		}
	}
	
	private static String stompMarks(final String s) {
		return java.text.Normalizer.normalize(s, java.text.Normalizer.Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}+",""); // from: http://stackoverflow.com/questions/285228/how-to-convert-utf-8-to-us-ascii-in-java
	}
	
	public static String plumpColumnName(final String kin, final Set<String> seen) {
		String k = kin;
		final int colonIndex = k.indexOf(':');
		if(colonIndex>0) { 		// get rid of any trailing : type info
			k = k.substring(0,colonIndex);
		}		
		k = stompMarks(k).replaceAll("\\W+"," ").trim().replaceAll("\\s+","_");
		if((k.length()<=0)||invalidColumnNames.contains(k.toLowerCase())||(!Character.isLetter(k.charAt(0)))) {
			k = columnPrefix + k;
		}
		if(seen.contains(k.toLowerCase())) {
			int i = 2;
			while(true) {
				String kt = k + "_" + i;
				if(!seen.contains(kt.toLowerCase())) {
					k = kt;
					break;
				} else {
					++i;
				}
			}
		}
		seen.add(k.toLowerCase());
		return k;
	}
	

	
	public static void loadTable(final Iterable<BurstMap> source, final RowCritique gateKeeper,
			final String tableName, final DBHandle handle) throws SQLException {
		// scan once to get field names and sizes and types
		final Pattern doubleRegexp = Pattern.compile("[-+]?[0-9]*\\.?[0-9]*([eE][-+]?[0-9]+)?"); // TODO: add missig values and Nan
		final Pattern intRegexp = Pattern.compile("[-+]?[0-9]+");
		final ArrayList<String> keys = new ArrayList<String>();
		boolean[] isInt = null;
		boolean[] isNumeric = null;
		int[] sizes = null;
		for(final BurstMap row: source) {
			if((gateKeeper==null)||(gateKeeper.accept(row))) {
				if(keys.isEmpty()) {
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
					String v = row.getAsString(k);
					if(v!=null) {
						v = v.trim();
						final int vlength = v.length();
						if((vlength>0)&&(!BurstMap.missingValue(v))) {
							sizes[i] = Math.max(sizes[i],vlength+1);
							if(isNumeric[i]) {
								if((vlength>38)||(!doubleRegexp.matcher(v).matches())) {
									isNumeric[i] = false;
								}
							}
							if(isInt[i]) {
								if((vlength>40)||(!intRegexp.matcher(v).matches())) {
									isInt[i] = false;
								}
							}
						}
					}
					++i;
				}
			}
		}
		// build SQL
		final String createStatement;
		final String insertStatement;
		final String selectStatement;
		{ 
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
					if(isInt[i]) {
						createBuilder.append(" " + colQuote + colName  + colQuote + " BIGINT");
					} else if(isNumeric[i]) {
						createBuilder.append(" " + colQuote + colName + colQuote + " DOUBLE PRECISION");
					} else {
						createBuilder.append(" " + colQuote + colName + colQuote + " VARCHAR(" + sizes[i] + ")");
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
		// set up table
		final int[] columnTypeCode;
		final String[] columnClassName;
		{
			final Statement stmt = handle.conn.createStatement();
			try {
				stmt.executeUpdate("DROP TABLE " + tableName);
			} catch (Exception ex) {
			}
			System.out.println("\texecuting: " + createStatement);
			stmt.executeUpdate(createStatement);
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
		{ // scan again and populate
			System.out.println("\texecuting: " + insertStatement);
			final PreparedStatement stmtA = handle.conn.prepareStatement(insertStatement);
			long reportTarget = 100;
			long nInserted = 0;
			for(final BurstMap row: source) {
				if((gateKeeper==null)||(gateKeeper.accept(row))) {
					int i = 0;
					for(final String k: keys) {
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
						++i;
					}
					stmtA.executeUpdate();
					++nInserted;
					if(nInserted>=reportTarget) {
						System.out.println("\twrote " + nInserted + "\t" + new Date());
						reportTarget *= 2;
					}
				}
			}
			System.out.println("\tdone, wrote " + nInserted + "\t" + new Date());
			stmtA.close();
		}
	}
}
