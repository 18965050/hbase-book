package client;

import java.io.IOException;

// cc AppendExample Example application appending data to a column in  HBase

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

public class AppendExample {

	public static void main(String[] args) throws IOException {

		System.setProperty("hadoop.home.dir", "D:/installed/hadoop-2.5.2");
		Configuration conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum", "centOS1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", 100, "colfam1", "colfam2");
		helper.put("testtable", new String[] { "row1" }, new String[] { "colfam1" }, new String[] { "qual1" },
				new long[] { 1 }, new String[] { "oldvalue" });
		System.out.println("Before append call...");
		helper.dump("testtable", new String[] { "row1" }, null, null);

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));

		// vv AppendExample
		Append append = new Append(Bytes.toBytes("row1"));
		append.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("newvalue"));
		append.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("anothervalue"));

		table.append(append);
		// ^^ AppendExample
		System.out.println("After append call...");
		helper.dump("testtable", new String[] { "row1" }, null, null);
		table.close();
		connection.close();
		helper.close();
	}
}
