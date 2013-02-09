package com.winvector.db;

import java.io.File;
import java.net.URI;
import java.sql.SQLException;
import java.util.Date;

import com.winvector.db.DBUtil.DBHandle;
import com.winvector.util.BurstMap;
import com.winvector.util.RowCritique;
import com.winvector.util.TrivialReader;


public class LoadTable {
	
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



	
	

	
	
	
	public static void loadTable(final Iterable<BurstMap> source, final RowCritique gateKeeper,
			final String tableName, final DBHandle handle) throws SQLException {
		final TableControl tableControl = new TableControl(tableName);
		tableControl.scanForDefs(source, gateKeeper);
		tableControl.buildSQLStatements();
		tableControl.createTable(handle);
		final long nInserted = tableControl.loadData(source, gateKeeper, handle);
		System.out.println("\tdone, wrote " + nInserted + "\t" + new Date());
	}
}
