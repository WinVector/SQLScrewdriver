package com.winvector.db;

import java.io.File;
import java.net.URI;
import java.sql.Connection;
import java.util.Date;
import java.util.Random;

import com.winvector.db.DBUtil.DBHandle;
import com.winvector.util.BurstMap;
import com.winvector.util.TrivialReader;

public class LoadFiles {
	public static void main(final String[] args) throws Exception {
		final Date now = new Date();
		System.out.println("start LoadFiles\t" + now);
		System.out.println("\tfor details see: http://www.win-vector.com/blog/2011/01/sql-screwdriver/");
		System.out.println("\tfor latest version see: https://github.com/WinVector/SQLScrewdriver");
		final int firstSourceArg = 3;
		if(args.length<4) {
			throw new Exception("use: LoadFiles dbProbsURI sepChar tableName inputURI+");
		}
		final URI propsURI = new URI(args[0]);
		final char sep = args[1].charAt(0);
		final String tableName = args[2];
		System.out.println("\tcwd: " + (new File(".")).getAbsolutePath());
		System.out.println("\tDBProperties XML:\t" + propsURI.toString());
		System.out.println("\tsep: " + sep);
		System.out.println("\ttableName:\t" + tableName);
		final DBHandle handle = DBUtil.buildConnection(propsURI,false);
		System.out.println("\tdb:\t" + handle);
		try {
			handle.conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		} catch (Exception ex) {
		}
		try {
			handle.conn.setAutoCommit(false);
		} catch (Exception ex) {
		}
		final TableControl tableControl = new TableControl(tableName);
		for(int argi=firstSourceArg;argi<args.length;++argi) {
			final URI inURI = new URI(args[argi]);
			System.out.println("\tscan source URI:\t" + inURI);
			final Iterable<BurstMap> source = new TrivialReader(inURI,sep,null,true,null, false);
			tableControl.scanForDefs(inURI.toString(),source, null);
		}
		tableControl.buildSQLStatements();
		tableControl.createTable(handle);
		final Random rand = new Random();
		long nInserted = 0;
		for(int argi=firstSourceArg;argi<args.length;++argi) {
			final URI inURI = new URI(args[argi]);
			System.out.println("\tload source URI:\t" + inURI);
			final Iterable<BurstMap> source = new TrivialReader(inURI,sep,null,true,null, false);
			final long nI = tableControl.loadData(inURI.toString(),now,rand,source, null, handle);
			System.out.println("\tinserted " + nI + " rows");
			nInserted += nI;
		}
		handle.conn.close();
		System.out.println("\tdone, wrote " + nInserted + " rows\t" + new Date());
		System.out.println("done LoadFiles\t" + new Date());
	}
}
