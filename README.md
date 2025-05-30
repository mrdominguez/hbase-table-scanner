# hbase-table-scanner

AUTHOR: Mariano Dominguez  
<marianodominguez@hotmail.com>  
https://www.linkedin.com/in/marianodominguez

VERSION: 1.0

FEEDBACK/BUGS: Please contact me by email.

## Description
HBase client to list and scan tables.

- Initial release
- List:
  - Support for table name pattern as argument: `-l,--list`
    - List all tables if no argument is provided 
  - Display table names with the option to also show descriptors: `-d,--descriptors`
- Scan:
  - Row limit output: `--limit` (default: 100)
  - Custom batch size: `-b,--batchSize`
  - Output cells: `-c,--cell`

_TODO: Add Kerberos support_

## Custom Timeouts and Retries
Update the following settings found in the code:
```
conf.set("hbase.cells.scanned.per.heartbeat.check", "10000");
conf.set("hbase.client.pause", "1000");
conf.set("hbase.client.retries.number", "2");
conf.set("hbase.client.scanner.timeout.period", "10000");
conf.set("hbase.rpc.timeout", "10000");
conf.set("zookeeper.recovery.retry", "1");
conf.set("zookeeper.session.timeout", "10000");
```

## Compilation and Usage
List of JAR files used to compile and test the code:
```
commons-cli-1.3.jar
commons-codec-1.15.jar
commons-collections-3.2.2.jar
commons-configuration2-2.8.0.jar
commons-lang3-3.12.0.jar
commons-logging-1.1.3.jar
hadoop-auth-3.3.3.jar
hadoop-common-3.3.3.jar
hadoop-shaded-guava-1.1.1.jar
hbase-client-2.4.15.jar
hbase-common-2.4.15.jar
hbase-protocol-2.4.15.jar
hbase-protocol-shaded-2.4.15.jar
hbase-shaded-miscellaneous-4.1.2.jar
hbase-shaded-netty-4.1.2.jar
hbase-shaded-protobuf-4.1.2.jar
hbase-unsafe-4.1.2.jar
htrace-core4-4.2.0-incubating.jar
metrics-core-3.2.6.jar
protobuf-java-2.5.0.jar
slf4j-api-1.7.36.jar
stax2-api-4.2.1.jar
woodstox-core-5.4.0.jar
zookeeper-3.5.10.jar
```
```
$ javac -cp *:. HBaseTableScanner.java && java -cp *:. HBaseTableScanner -h

usage: HBaseTableScanner [-b <arg>] [-c] [-d] [-h] [-l <arg>] [--limit <arg>]
       [-t <arg>] [--zkPort <arg>] [--zkQuorum <arg>] [--znode <arg>]
 -b,--batchSize <arg>   Batch size
 -c,--cell              Cell output
 -d,--descriptors       Display descriptors (list tables)
 -h,--help              Display usage
 -l,--list <arg>        *List tables (arg -pattern- is optional)
    --limit <arg>       Row limit (default: 100)
 -t,--table <arg>       *Table name
    --zkPort <arg>      Zookeeper port (default: 2181)
    --zkQuorum <arg>    Zookeeper quorum (default: localhost)
    --znode <arg>       HBase znode (default: /hbase)
```

## Sample Output
### List `SYSTEM` tables
```
$ java -cp HBaseTableScanner.jar HBaseTableScanner -zkQuorum ***** -l "SYSTEM:.*"
SYSTEM:CATALOG
SYSTEM:CHILD_LINK
SYSTEM:FUNCTION
SYSTEM:LOG
SYSTEM:MUTEX
SYSTEM:SEQUENCE
SYSTEM:STATS
SYSTEM:TASK
---
Total tables: 8
Master address: *****:16000
```

### Scan table
```
$ java -cp HBaseTableScanner.jar HBaseTableScanner -zkQuorum ***** -t SYSTEM:CATALOG
0: keyvalues={...}
.
.
.
99: keyvalues={...}
---
Total rows: 100
Master address: *****:16000
```
