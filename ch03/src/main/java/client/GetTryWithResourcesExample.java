package client;

// cc GetTryWithResourcesExample Example application retrieving data from HBase using a Java 7 construct
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

public class GetTryWithResourcesExample {

	public static void main(String[] args) throws IOException {
		// vv GetTryWithResourcesExample

		// Configuration conf=new Configuration();
		/**
		 * 需要下载winutils.exe并设置环境变量, 否则报Could not locate executable null\bin\winutils.exe
		 */
		System.setProperty("hadoop.home.dir", "D:/installed/hadoop-2.5.2");
		Configuration conf = HBaseConfiguration.create(); // co
															// GetTryWithResourcesExample-1-CreateConf
															// Create the
															// configuration.
		conf.set("hbase.zookeeper.quorum", "centOS1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "centOS1:60000");
		conf.set("hbase.rootdir", "hdfs://centOS1:9000/hbase");

		// conf.set("hbase.master", "centOS1:600000");

		// ^^ GetTryWithResourcesExample
		HBaseHelper helper = HBaseHelper.getHelper(conf);

		if (!helper.existsTable("testtable")) {
			helper.createTable("testtable", "colfam1");
		}
		// vv GetTryWithResourcesExample
		try (Connection connection = ConnectionFactory.createConnection(conf);
				Table table = connection.getTable(TableName.valueOf("testtable")); // co
																					// GetTryWithResourcesExample-2-NewTable
																					// Instantiate
																					// a
																					// new
																					// table
																					// reference
																					// in
																					// "try"
																					// block.
		) {
			Get get = new Get(Bytes.toBytes("row1"));
			get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
			Result result = table.get(get);
			byte[] val = result.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
			System.out.println("Value: " + Bytes.toString(val));
		} // co GetTryWithResourcesExample-3-Close No explicit close needed,
			// Java will handle AutoClosable's.
			// ^^ GetTryWithResourcesExample
		helper.close();
	}
}
