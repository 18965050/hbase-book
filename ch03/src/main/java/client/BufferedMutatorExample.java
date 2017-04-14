package client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc BufferedMutatorExample Shows the use of the client side write buffer
public class BufferedMutatorExample {

	private static final Log LOG = LogFactory.getLog(BufferedMutatorExample.class);

	// vv BufferedMutatorExample
	private static final int POOL_SIZE = 10;
	private static final int TASK_COUNT = 100;
	private static final TableName TABLE = TableName.valueOf("testtable");
	private static final byte[] FAMILY = Bytes.toBytes("colfam1");

	public static void main(String[] args) throws Exception {

		System.setProperty("hadoop.home.dir", "D:/installed/hadoop-2.5.2");
		Configuration configuration = HBaseConfiguration.create();

		configuration.set("hbase.zookeeper.quorum", "centOS1");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		// ^^ BufferedMutatorExample
		HBaseHelper helper = HBaseHelper.getHelper(configuration);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1");
		// vv BufferedMutatorExample
		BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
			@Override
			public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
				for (int i = 0; i < e.getNumExceptions(); i++) {
					LOG.info("Failed to sent put: " + e.getRow(i));
				}
			}
		};
		BufferedMutatorParams params = new BufferedMutatorParams(TABLE).listener(listener);

		try (Connection conn = ConnectionFactory.createConnection(configuration);
				BufferedMutator mutator = conn.getBufferedMutator(params)) {
			ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
			List<Future<Void>> futures = new ArrayList<>(TASK_COUNT);

			for (int i = 0; i < TASK_COUNT; i++) {
				futures.add(workerPool.submit(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						Put p = new Put(Bytes.toBytes("row1"));
						p.addColumn(FAMILY, Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
						mutator.mutate(p);
						// [...]
						// Do work... Maybe call mutator.flush() after many
						// edits to ensure
						// any of this worker's edits are sent before exiting
						// the Callable
						return null;
					}
				}));
			}

			for (Future<Void> f : futures) {
				f.get(5, TimeUnit.MINUTES); // co
											// BufferedMutatorExample-09-Shutdown
											// Wait for workers and shut down
											// the pool.
			}
			workerPool.shutdown();
		} catch (IOException e) { // co BufferedMutatorExample-10-ImplicitClose
									// The try-with-resource construct ensures
									// that first the mutator, and then the
									// connection are closed. This could trigger
									// exceptions and call the custom listener.
			LOG.info("Exception while creating or freeing resources", e);
		}
	}
}