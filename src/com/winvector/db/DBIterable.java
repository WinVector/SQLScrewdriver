package com.winvector.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import com.winvector.db.DBUtil.DBHandle;
import com.winvector.db.SQLTypeAdapter.FieldExtracter;
import com.winvector.util.BurstMap;
import com.winvector.util.HBurster;


public final class DBIterable implements Iterable<BurstMap> {
	private Statement stmt;
	private final String query;
	
	public DBIterable(final Statement stmt, final String query) {
		this.stmt = stmt;
		this.query = query;
	}
	
	
	public static final class TypeInfo {
		public final String columnName;
		public final String columnLabel;
		public final String tableName;
		public final String javaClassName;
		public final int sqlColumnType; 
		public final FieldExtracter extracter;
		
		public TypeInfo(final ResultSetMetaData rsm, final int idx) throws SQLException {
			javaClassName = rsm.getColumnClassName(idx);
			sqlColumnType = rsm.getColumnType(idx);
			columnName = rsm.getColumnName(idx);
			columnLabel = rsm.getColumnLabel(idx);
			tableName = rsm.getTableName(idx);
			extracter = SQLTypeAdapter.pickExtracter(sqlColumnType,javaClassName);
		}
	}
	

	public static final class RSIterator implements Iterator<BurstMap> {
		private BurstMap next = null;
		private ResultSet rs;
		private final String[] colNames;
		private final TypeInfo[] colInfos;
		
		public RSIterator(final ResultSet rs) throws SQLException {
			this.rs = rs;
			if(rs.next()) {
				final ResultSetMetaData meta = rs.getMetaData();
				final int n = meta.getColumnCount();
				final String[] origColNames = new String[n];
				colInfos = new TypeInfo[n];
				for(int i=0;i<n;++i) {
					// could also prepend (when appropriate) meta.getTableName(i+1);
					//origColNames[i] = meta.getColumnName(i+1);
					origColNames[i] = meta.getColumnLabel(i+1);
					colInfos[i] = new TypeInfo(meta,i+1);
				}
				colNames = HBurster.buildHeaderFlds(origColNames);
			} else {
				rs.close();
				this.rs = null;
				colNames = null;
				colInfos = null;
			}
			advance();
		}

				
		private void advance() {
			next = null;
			if(rs!=null) {
				try {
					int n = colNames.length;
					final Map<String,Object> mp = new LinkedHashMap<String,Object>();
					for(int i=0;i<n;++i) {
						Object field = null;
						try {
							field = colInfos[i].extracter.getField(rs,i);
						} catch (Exception ex) {
						}
						if(null==field) { // belt and suspenders version
							field = SQLTypeAdapter.stringExtracter.getField(rs,i);
						}
						mp.put(colNames[i],field);
					}
					next = new BurstMap("db",mp); 
					if(!rs.next()) {
						rs = null;
					}
				} catch (SQLException ex) {
					if(rs!=null) {
						try {
							rs.close();
						} catch (SQLException cx) {
						}
					}
					rs = null;
					next = null;
					throw new RuntimeException(ex);
				}
			}
		}

		@Override
		public boolean hasNext() {
			return next!=null;
		}

		@Override
		public BurstMap next() {
			if(!hasNext()) {
				throw new NoSuchElementException("RSIterator");
			}
			final BurstMap ret = next;
			advance();
			return ret;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("RSterator");
		}

	}

	@Override
	public Iterator<BurstMap> iterator() {
		try {
			final ResultSet rs = stmt.executeQuery(query);
			return new RSIterator(rs);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public String toString() {
		return query;
	}

	public static Iterable<BurstMap> buildSource(final DBHandle handle,
			final Statement stmt, final String dbTable, final Iterable<String> terms) {
		final StringBuilder query = new StringBuilder();
		query.append("SELECT ");
		{
			boolean first = true;
			for(final String term: terms) {
				if(first) {
					first = false;
				} else {
					query.append(",");
				}
				query.append(term);
			}
		}
		query.append(" FROM ");
		query.append(dbTable);
		return new DBIterable(stmt,query.toString());
	}
}

