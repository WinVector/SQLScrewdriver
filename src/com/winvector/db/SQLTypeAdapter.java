package com.winvector.db;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * try to deal with mapping declared JDBC types into usable Java types
 * @author jmount
 *
 */
public final class SQLTypeAdapter {
	public static interface FieldExtracter {
		public Object getField(final ResultSet rs, final int i) throws SQLException;
	}

	
	
	public static final FieldExtracter stringExtracter =  new FieldExtracter() {
		@Override
		public Object getField(final ResultSet rs, final int i) throws SQLException {
			return rs.getString(i+1);
		}
	 };

	 public static final FieldExtracter binaryExtracter =  new FieldExtracter() {
		 @Override
		 public Object getField(final ResultSet rs, final int i) throws SQLException {
			 return rs.getString(i+1);
		}
	 };
	 
	 
	private static final Map<Integer,FieldExtracter> extracters = new HashMap<Integer,FieldExtracter>();


	static {
		 extracters.put(java.sql.Types.BIT, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getInt(i+1);
			}
		 });
		 extracters.put(java.sql.Types.TINYINT, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getInt(i+1);
			}
		 });
		 extracters.put(java.sql.Types.SMALLINT, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getInt(i+1);
			}
		 });
		 extracters.put(java.sql.Types.INTEGER, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getInt(i+1);
			}
		 });
		 extracters.put(java.sql.Types.BIGINT, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getBigDecimal(i+1);
			}
		 });
		 extracters.put(java.sql.Types.FLOAT, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getDouble(i+1);
			}
		 });
		 extracters.put(java.sql.Types.REAL, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getDouble(i+1);
			}
		 });
		 extracters.put(java.sql.Types.DOUBLE, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getDouble(i+1);
			}
		 });
		 extracters.put(java.sql.Types.NUMERIC, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getString(i+1); // Note: try not to map to this one
			}
		 });
		 extracters.put(java.sql.Types.DECIMAL, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getBigDecimal(i+1);
			}
		 });
		 extracters.put(java.sql.Types.CHAR,stringExtracter);
		 extracters.put(java.sql.Types.VARCHAR,stringExtracter);
		 extracters.put(java.sql.Types.LONGVARCHAR,stringExtracter);
		 extracters.put(java.sql.Types.DATE, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getDate(i+1);
			}
		 });
		 extracters.put(java.sql.Types.TIME, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getTime(i+1);
			}
		 });
		 extracters.put(java.sql.Types.TIMESTAMP, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getTimestamp(i+1);
			}
		 });
		 extracters.put(java.sql.Types.BINARY, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getString(i+1);
			}
		 });
		 extracters.put(java.sql.Types.VARBINARY,binaryExtracter);
		 extracters.put(java.sql.Types.LONGVARBINARY,binaryExtracter);
		 extracters.put(java.sql.Types.NULL, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return null;
			}
		 });
		 extracters.put(java.sql.Types.OTHER,stringExtracter);
		 extracters.put(java.sql.Types.JAVA_OBJECT,binaryExtracter);
		 extracters.put(java.sql.Types.DISTINCT, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getString(i+1);
			}
		 });
		 extracters.put(java.sql.Types.STRUCT,binaryExtracter);
		 extracters.put(java.sql.Types.ARRAY,binaryExtracter);
		 extracters.put(java.sql.Types.BLOB,binaryExtracter);
		 extracters.put(java.sql.Types.CLOB,binaryExtracter);
		 extracters.put(java.sql.Types.REF,stringExtracter);
		 extracters.put(java.sql.Types.DATALINK,binaryExtracter);
		 extracters.put(java.sql.Types.BOOLEAN, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getBoolean(i+1);
			}
		 });
		 extracters.put(java.sql.Types.ROWID, new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getLong(i+1);
			}
		 });
		 extracters.put(java.sql.Types.NCHAR,stringExtracter);
		 extracters.put(java.sql.Types.NVARCHAR,stringExtracter);
		 extracters.put(java.sql.Types.LONGNVARCHAR,stringExtracter);
		 extracters.put(java.sql.Types.NCLOB,stringExtracter);
		 extracters.put(java.sql.Types.SQLXML,stringExtracter);
	}
	
	private static Map<String,FieldExtracter> numericTypes = new HashMap<String,FieldExtracter>();
	
	static {
		numericTypes.put(Integer.class.getName(),new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getInt(i+1);
			}
		});
		numericTypes.put(AtomicInteger.class.getName(),new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getInt(i+1);
			}
		});
		numericTypes.put(Long.class.getName(),new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getLong(i+1);
			}
		});
		numericTypes.put(AtomicLong.class.getName(),new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getLong(i+1);
			}
		});
		numericTypes.put(BigDecimal.class.getName(),new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getBigDecimal(i+1);
			}
		});
		numericTypes.put(BigInteger.class.getName(),new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getBigDecimal(i+1);
			}
		});
		numericTypes.put(Float.class.getName(),new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getDouble(i+1);
			}
		});
		numericTypes.put(Double.class.getName(),new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getDouble(i+1);
			}
		});
		numericTypes.put(Short.class.getName(),new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getInt(i+1);
			}
		});
		numericTypes.put(Byte.class.getName(),new FieldExtracter() {
			@Override
			public Object getField(final ResultSet rs, final int i) throws SQLException {
				return rs.getInt(i+1);
			}
		});
	}

	public static FieldExtracter pickExtracter(final int sqlColumnType, final String javaClassName) {
		if(sqlColumnType==java.sql.Types.NUMERIC) {
			if(null==javaClassName) {
				return stringExtracter;
			}
			final FieldExtracter cfound = numericTypes.get(javaClassName);
			if(null!=cfound) {
				return cfound;
			} else {
				// give up on being clever
				return stringExtracter;
			}
		}
		final FieldExtracter found = extracters.get(sqlColumnType);
		if(null!=found) {
			return found;
		} else {
			return stringExtracter;
		}
	}
	
}
