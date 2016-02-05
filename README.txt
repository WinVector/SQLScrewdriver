
Update: 2-5-2016

This package is getting a bit old (though we do use it for some projects).
I just added a "colquote" key to the db connection properties/xml (see DBUtil.java line 19) to close an issue in using this tool with MySQL.

For our current advice on semi-generic methods for loading data (such as dbWriteTable() in R) please see here http://www.win-vector.com/blog/2016/02/using-postgresql-in-r/ .

3-27-2012

Crude SQL dump/restore tool (database agnostic, uses JDBC).

Described at: http://www.win-vector.com/blog/2011/01/sql-screwdriver/

Example use: dump from one PostgreSQL DB and restore to another (not all column types preserved).

(SQLScrewdriver.jar comes from https://github.com/WinVector/SQLScrewdriver/blob/master/SQLScrewdriver.jar or https://github.com/WinVector/SQLScrewdriver and postgresql.jar is the appropriate DB driver from http://jdbc.postgresql.org/download.html )

# dump the table to TSV from remote database
java -cp SQLScrewdriver.jar:postgresql.jar com.winvector.db.DBDump file:srvrdb.xml "SELECT * from model_res_summary" model_res_summary.tsv

# load the TSV into local database
java -cp SQLScrewdriver.jar:postgresql.jar com.winvector.db.LoadTable file:db.xml t file:model_res_summary.tsv model_res_summary

and srvdb.xml is:

<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
 <entry key="user">am</entry>
 <entry key="url">jdbc:postgresql://192.168.1.9:5432/am</entry>
 <entry key="password"></entry>
 <entry key="driver">org.postgresql.Driver</entry>
</properties>

and db.xml is:

<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
 <entry key="user">miner_demo</entry>
 <entry key="url">jdbc:postgresql://localhost:5432/miner_demo</entry>
 <entry key="password"></entry>
 <entry key="driver">org.postgresql.Driver</entry>
</properties>

with passwords filled in.

Additional property keys can set the SQL types used to populate integer types, double types and timestamp types.  The defaults are:

 <entry key="SQLDOUBLE">DOUBLE PRECISION</entry>
 <entry key="SQLINT">BIGINT</entry>
 <entry key="SQLTIME">TIMESTAMP</entry>

For Oracle databases we suggest adding the following line to your db properties files:

 <entry key="SQLINT">NUMBER</entry>


The units tests in the test directory can be run if you include Junit and H2 database jars.
