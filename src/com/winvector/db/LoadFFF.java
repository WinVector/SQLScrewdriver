package com.winvector.db;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URI;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;

import com.winvector.db.DBUtil.DBHandle;
import com.winvector.util.BurstMap;
import com.winvector.util.TrivialReader;

public final class LoadFFF {
	public static final class FixedFieldDef {
		public final int l;
		public final int r;
		public final String name;
		
		public FixedFieldDef(final int l,
			 final int r,
			 final String name) {
			this.l = l;
			this.r = r;
			this.name = name;
		}
		
		@Override
		public String toString() {
			return name + "_[" + l + "," + r + ")";
		}
	}
	
	public static final class FFReader implements Iterable<BurstMap> {
		private final ArrayList<FixedFieldDef> fields;
		private final URI source;
		
		public FFReader(final ArrayList<FixedFieldDef> fields, final URI source) {
			this.fields = fields;
			this.source = source;
		}
		
		private final class FFIt implements Iterator<BurstMap> {
			private BurstMap next = null;
			private LineNumberReader rdr = null;
			
			public FFIt() throws IOException {
				rdr = TrivialReader.openBufferedReader(source,null);
				advance();
			}
			
			private void advance() throws IOException {
				next = null;
				while((next==null)&&(null!=rdr)) {
					final String line = rdr.readLine();
					if(null==line) {
						rdr.close();
						rdr = null;
					} else {
						if(line.trim().length()>0) {
							final Map<String,Object> map = new LinkedHashMap<String,Object>();
							final int lineLen = line.length();
							boolean gotOne = false;
							for(final FixedFieldDef fi: fields) {
								// numbering from zero
								if((fi.l<lineLen)&&(fi.r>fi.l)) {
									final String val = line.substring(fi.l,Math.min(fi.r,lineLen)).trim();
									gotOne |= val.length()>0;
									map.put(fi.name,val);
								} else {
									map.put(fi.name,null);
								}
							}
							if(gotOne) {
								next = new BurstMap(line,map);
							}
						}
					}
				}
			}
			
			@Override
			public boolean hasNext() {
				return null!=next;
			}

			@Override
			public BurstMap next() {
				if(!hasNext()) {
					throw new NoSuchElementException();
				}
				final BurstMap r = next;
				try {
					advance();
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}
				return r;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public String toString() {
				return source.toString();  
			}			
		}

		@Override
		public Iterator<BurstMap> iterator() {
			try {
				return new FFIt();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		@Override
		public String toString() {
			return source.toString();
		}
	}
	
	private static ArrayList<FixedFieldDef> readFieldDefs(final Iterable<BurstMap> defSource) {
		final ArrayList<FixedFieldDef> fieldDefs = new ArrayList<FixedFieldDef>();
		for(final BurstMap di: defSource) {
			final String posStr = di.getAsString("Position");
			final String fieldName = di.getAsString("Field");
			final String[] range = posStr.split("-",2);
			final int l;
			final int r;
			// external numbering starts form 1 and is inclusive
			if(range.length>1) {
				l = Integer.parseInt(range[0])-1;
				r = Integer.parseInt(range[1]);
			} else {
				l = Integer.parseInt(posStr)-1;
				r = l + 1;
			}
			fieldDefs.add(new FixedFieldDef(l,r,fieldName));
		}
		return fieldDefs;
	}
	
	private static ArrayList<FixedFieldDef> notCoveredBySingles(ArrayList<FixedFieldDef> defs) {
		final ArrayList<FixedFieldDef> r = new ArrayList<FixedFieldDef>();
		final int defsSize = defs.size();
		final BitSet take = new BitSet(defsSize);
		final BitSet covered = new BitSet();
		for(int i=0;i<defsSize;++i) {
			final FixedFieldDef di = defs.get(i);
			if(di.r==di.l+1) {
				if(!covered.get(di.l)) {
					covered.set(di.l);
					take.set(i);
				}
			}
		}
		for(int i=0;i<defsSize;++i) {
			final FixedFieldDef di = defs.get(i);
			if(di.r>di.l+1) {
				boolean mis = false;
				for(int j=di.l;(!mis)&&(j<di.r);++j) {
					if(!covered.get(j)) {
						mis = true;
					}
				}
				if(mis) {
					take.set(i);
				}
			}
		}
		for(int i=0;i<defsSize;++i) {
			if(take.get(i)) {
				final FixedFieldDef di = defs.get(i);
				r.add(di);
			}
		}
		return r;
	}
	
	public static void main(final String[] args) throws Exception {
		final Date now = new Date();
		System.out.println("start LoadFFF\t" + now);
		System.out.println("\tfor details see: http://www.win-vector.com/blog/2011/01/sql-screwdriver/");
		System.out.println("\tfor latest version see: https://github.com/WinVector/SQLScrewdriver");
		final int firstSourceArg = 3;
		if(args.length<4) {
			throw new Exception("use: LoadFFF dbProbsURI fieldSpecURI.tsv tableName inputURI+");
		}
		final URI propsURI = new URI(args[0]);
		final URI fieldSpecURI = new URI(args[1]);
		final String tableName = args[2];
		System.out.println("\tcwd: " + (new File(".")).getAbsolutePath());
		System.out.println("\tDBProperties XML:\t" + propsURI.toString());
		System.out.println("\ttableName:\t" + tableName);
		final Properties props = DBUtil.loadProps(propsURI);
		final DBHandle handle = DBUtil.buildConnection(propsURI.toString(),props,false);
		System.out.println("\tdb:\t" + handle);
		final ArrayList<FixedFieldDef> origFieldDefs = readFieldDefs(new TrivialReader(fieldSpecURI,'\t',null,true,null, false));
		final ArrayList<FixedFieldDef> fieldDefs = notCoveredBySingles(origFieldDefs);
		try {
			handle.conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		} catch (Exception ex) {
		}
		try {
			handle.conn.setAutoCommit(false);
		} catch (Exception ex) {
		}
		final TableControl tableControl = new TableControl(props,tableName);
		final ArrayList<Iterable<BurstMap>> sources = new ArrayList<Iterable<BurstMap>>();
		for(int argi=firstSourceArg;argi<args.length;++argi) {
			final URI inURI = new URI(args[argi]);
			final Iterable<BurstMap> source = new FFReader(fieldDefs,inURI);
			sources.add(source);
		}
		for(final Iterable<BurstMap> source: sources) {
			System.out.println("\tscan source URI:\t" + source.toString());
			tableControl.scanForDefs(source.toString(),source, null);
		}
		tableControl.buildSQLStatements();
		tableControl.createTable(handle);
		final Random rand = new Random(42);
		long nInserted = 0;
		for(final Iterable<BurstMap> source: sources) {
			System.out.println("\tload source URI:\t" + source);
			final long nI = tableControl.loadData(source.toString(),now,rand,source, null, handle);
			System.out.println("\tinserted " + nI + " rows");
			nInserted += nI;
		}
		handle.conn.close();
		System.out.println("\tdone, wrote " + nInserted + " rows\t" + new Date());
		System.out.println("done LoadFFF\t" + new Date());
	}
}
