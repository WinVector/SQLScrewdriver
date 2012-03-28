
3-27-2012

Crude SQL dump/restore tool (database agnostic, uses JDBC).

Described at: http://www.win-vector.com/blog/2011/01/sql-screwdriver/

Example use: dump form one PostgreSQL DB and restore to another (not all column types preserved).

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
