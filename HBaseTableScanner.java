/*
 * Copyright 2025 Mariano Dominguez
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * HBase client to list and scan tables.
 * Author: Mariano Dominguez
 * Version: 1.0
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class HBaseTableScanner {

  private static final byte[] POSTFIX = new byte[] { 0x00 };

  public static void main(String[] args) throws IOException {

	HBaseTableScanner hbts = new HBaseTableScanner();
	Options options = new Options();

	Option zkQuorumOpt = new Option(null, "zkQuorum", true, "Zookeeper quorum (default: localhost)");
	zkQuorumOpt.setRequired(false);
	options.addOption(zkQuorumOpt);

	Option zkPortOpt = new Option(null, "zkPort", true, "Zookeeper port (default: 2181)");
	zkPortOpt.setRequired(false);
	options.addOption(zkPortOpt);

	Option znodeOpt = new Option(null, "znode", true, "HBase znode (default: /hbase)");
	znodeOpt.setRequired(false);
	options.addOption(znodeOpt);

	Option tableNameOpt = new Option("t", "table", true, "*Table name");
	tableNameOpt.setRequired(false);
	options.addOption(tableNameOpt);
	
	Option limitOpt = new Option(null, "limit", true, "Row limit (default: 100)");
	limitOpt.setRequired(false);
	options.addOption(limitOpt);

	Option cellOpt = new Option("c", "cell", false, "Cell output");
	cellOpt.setRequired(false);
	options.addOption(cellOpt);

	Option batchSizeOpt = new Option("b", "batchSize", true, "Batch size");
	batchSizeOpt.setRequired(false);
	options.addOption(batchSizeOpt);

	Option listOpt = new Option("l", "list", true, "*List tables (arg -pattern- is optional)");
	listOpt.setRequired(false);
	listOpt.setOptionalArg(true);
	options.addOption(listOpt);

	Option descriptorsOpt = new Option("d", "descriptors", false, "Display descriptors (list tables)");
	descriptorsOpt.setRequired(false);
	options.addOption(descriptorsOpt);

	Option helpOpt = new Option("h", "help", false, "Display usage");
	helpOpt.setRequired(false);
	options.addOption(helpOpt);

	CommandLineParser parser = new DefaultParser();
	HelpFormatter formatter = new HelpFormatter();
	CommandLine cmd = null;

	try {
		cmd = parser.parse(options, args);
	} catch (ParseException e) {
		System.out.println(e.getMessage());
		formatter.printHelp(80, hbts.getClass().getSimpleName(), null, options, null, true);
		System.exit(1);
	}

	if ( cmd.hasOption("help") ) {
		formatter.printHelp(80, hbts.getClass().getSimpleName(), null, options, null, true);
		System.exit(0);
	}

	String zkQuorum = cmd.hasOption("zkQuorum") ? cmd.getOptionValue("zkQuorum") : "localhost";
	String zkPort = cmd.hasOption("zkPort") ? cmd.getOptionValue("zkPort") : "2181";
	String znode = cmd.hasOption("znode") ? cmd.getOptionValue("znode") : "/hbase";
	String tableName = cmd.hasOption("table") ? cmd.getOptionValue("table") : null;
	int limit = cmd.hasOption("limit") ? Integer.parseInt(cmd.getOptionValue("limit")) : 100;
	Boolean cellOutput = cmd.hasOption("cell") ? true : false;
	String batchSize = cmd.hasOption("batchSize") ? cmd.getOptionValue("batchSize") : null;
	String list = cmd.hasOption("list") ? cmd.getOptionValue("list") == null ? ".*" : cmd.getOptionValue("list") : null;
	Boolean descriptors = cmd.hasOption("descriptors") ? true : false;
	String masterHost;
	int masterPort;

	try {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zkQuorum);
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		conf.set("zookeeper.znode.parent", znode);
		// timeout and retry parameters
		conf.set("hbase.cells.scanned.per.heartbeat.check", "10000");
		conf.set("hbase.client.pause", "1000");
		conf.set("hbase.client.retries.number", "2");
		conf.set("hbase.client.scanner.timeout.period", "10000");
		conf.set("hbase.rpc.timeout", "10000");
		conf.set("zookeeper.recovery.retry", "1");
		conf.set("zookeeper.session.timeout", "10000");

		if ( tableName == null && list == null ) {
			System.out.println("Missing required option: -l,--list | -t,--table");
			System.out.println("Display usage: -h,--help");
			System.exit(1);
		}

		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();

		masterHost = admin.getMaster().getAddress().getHostname();
		masterPort = admin.getMaster().getPort();
		int totalTables = 0;
		int totalRows = 0;

		if ( list != null ) {
			if ( descriptors ) {
				HTableDescriptor[] htds = admin.listTables(list);
				for ( HTableDescriptor htd : htds ) {
					System.out.println(htd);
					totalTables++;
				}
			} else {
				TableName[] tableNames = admin.listTableNames(list);
				for (TableName name : tableNames ) {
					System.out.println(name);
					totalTables++;
				}
			}
			System.out.println("---\nTotal tables: " + totalTables);
		} else {
			Table table = connection.getTable(TableName.valueOf(tableName));
			if ( cellOutput ) {
				ResultScanner scanner = table.getScanner(new Scan());
				outerloop:
				for ( Result result = scanner.next(); result != null; result = scanner.next() ) {
					for ( Cell cell : result.listCells() ) {
						String rkey = Bytes.toString(result.getRow());
						String cf = Bytes.toString(CellUtil.cloneFamily(cell));
						String qual = Bytes.toString(CellUtil.cloneQualifier(cell));
						String value = Bytes.toString(CellUtil.cloneValue(cell));
						System.out.println("Row key: " + rkey +
							", Column Family: " + cf +
							", Qualifier: " + qual +
							", Value : " + value);
						totalRows++;
						if ( totalRows == limit ) break outerloop;
					}
				}
			} else {
				byte[] lastRow = null;
				outerloop:
				while ( true ) {
					Scan scan = new Scan();
					if ( batchSize != null ) scan.setBatch(Integer.parseInt(batchSize));
					if ( lastRow != null ) {
						byte[] startRow = Bytes.add(lastRow, POSTFIX);
						System.out.println("Start row: " + Bytes.toStringBinary(startRow));
						scan.setStartRow(startRow);
					}
					ResultScanner scanner = table.getScanner(scan);
					int localRows = 0;
					Result result;
					while ( (result = scanner.next()) != null ) {
						System.out.println(localRows++ + ": " + result);
						totalRows++;
						if ( totalRows == limit ) break outerloop;
						lastRow = result.getRow();
					}
					scanner.close();
					if ( localRows == 0 ) break;
				}
			} 
			System.out.println("---\nTotal rows: " + totalRows);
		}
		if ( connection != null && !connection.isClosed() ) connection.close();
		System.out.println("Master address: " + masterHost + ":" + masterPort);
	} catch (IOException e) {
		e.printStackTrace();
	}
  }
}
