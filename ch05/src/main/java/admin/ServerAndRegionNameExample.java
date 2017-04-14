package admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc ServerAndRegionNameExample Shows the use of server and region names
public class ServerAndRegionNameExample {

	public static void main(String[] args) throws IOException, InterruptedException {

		System.setProperty("hadoop.home.dir", "D:/installed/hadoop-2.5.2");
		Configuration conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum", "centOS1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();

		// vv ServerAndRegionNameExample
		TableName tableName = TableName.valueOf("testtable");
		HColumnDescriptor coldef1 = new HColumnDescriptor("colfam1");
		HColumnDescriptor coldef2 = new HColumnDescriptor("colfam2");
		HTableDescriptor desc = new HTableDescriptor(tableName).addFamily(coldef1)
				.setValue("Description", "Chapter 5 - ServerAndRegionNameExample").addFamily(coldef2);
		byte[][] regions = new byte[][] { Bytes.toBytes("ABC"), Bytes.toBytes("DEF"), Bytes.toBytes("GHI"),
				Bytes.toBytes("KLM"), Bytes.toBytes("OPQ"), Bytes.toBytes("TUV") };
		admin.createTable(desc, regions);

		RegionLocator locator = connection.getRegionLocator(tableName);
		HRegionLocation location = locator.getRegionLocation(Bytes.toBytes("Foo"));
		HRegionInfo info = location.getRegionInfo();
		System.out.println("Region Name: " + info.getRegionNameAsString());
		System.out.println("Server Name: " + location.getServerName());
		// ^^ ServerAndRegionNameExample
		locator.close();
		admin.close();
		connection.close();
	}
}
