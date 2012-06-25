package com.globalmaksimum.cassandradeneme;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;

import me.prettyprint.cassandra.serializers.AsciiSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Hello world!
 * 
 */
public class Populate {
	private static Random RANDOM_CAMPAIGN = new Random(
			System.currentTimeMillis());
	private static Random RANDOM_CAMPAIGN_NAME = new Random(
			System.currentTimeMillis());

	public static void main(String[] args) {
		boolean drop = false;
		BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(20);
		ExecutorService executorService = new ThreadPoolExecutor(4, 4, 1,
				TimeUnit.HOURS, workQueue, new CallerRunsPolicy());
		if (args.length > 0 && args[0].equalsIgnoreCase("drop"))
			drop = true;
		Cluster myCluster = HFactory.getOrCreateCluster("Test Cluster",
				"localhost:9160");
		Keyspace createKeyspace = getKeySpace(myCluster, drop);
		long start = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			executorService.execute(new CustomerAdd(createKeyspace, String
					.format("%1$026d", i)));
			if (i % 1000 == 0)
				System.out.println("customer " + i);
		}
		executorService.shutdown();
		try {
			executorService.awaitTermination(3, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(String.format("completed in %1$d",
				System.currentTimeMillis() - start));
		HFactory.shutdownCluster(myCluster);
	}

	protected static Keyspace getKeySpace(Cluster myCluster, boolean drop) {
		KeyspaceDefinition keyspaceDefinition = myCluster
				.describeKeyspace("deneme");
		if (keyspaceDefinition != null && drop) {
			myCluster.dropKeyspace("deneme");
			addKeySpace(myCluster);
		}
		Keyspace createKeyspace = HFactory.createKeyspace("deneme", myCluster);
		return createKeyspace;
	}

	protected static void addKeySpace(Cluster myCluster) {
		List<ColumnFamilyDefinition> cfDefs = new ArrayList<ColumnFamilyDefinition>();
		cfDefs.add(HFactory.createColumnFamilyDefinition("deneme", "global",
				ComparatorType.getByClassName("DateType")));
		cfDefs.add(HFactory.createColumnFamilyDefinition("deneme", "campaign1",
				ComparatorType.getByClassName("DateType")));
		cfDefs.add(HFactory.createColumnFamilyDefinition("deneme", "campaign2",
				ComparatorType.getByClassName("DateType")));
		cfDefs.add(HFactory.createColumnFamilyDefinition("deneme", "campaign3",
				ComparatorType.getByClassName("DateType")));
		KeyspaceDefinition ksdef = HFactory.createKeyspaceDefinition("deneme",
				"org.apache.cassandra.locator.SimpleStrategy", 1, cfDefs);
		myCluster.addKeyspace(ksdef, true);
	}

	private static class CustomerAdd implements Runnable {
		private Keyspace keyspace;
		private String customer;

		public CustomerAdd(Keyspace keyspace, String customer) {
			this.keyspace = keyspace;
			this.customer = customer;
		}

		@Override
		public void run() {
			Calendar instance = Calendar.getInstance();
			instance.set(2012, 1, 1, 0, 0, 0);
			Date time = instance.getTime();
			Mutator<String> mutator = HFactory.createMutator(keyspace,
					new AsciiSerializer());

			for (int i = 0; i < 300; i++) {
				HColumn<Date, byte[]> column = HFactory.createColumn(time,
						new byte[] {}, DateSerializer.get(),
						BytesArraySerializer.get());
				String hitCampaignName = getHitCampaignName();
				if (hitCampaignName != null)
					mutator.addInsertion(customer, hitCampaignName, column);
				mutator.addInsertion(customer, "global", column);
				time = new Date(time.getTime() + 2000);
			}
			mutator.execute();

		}

	}

	private static String getHitCampaignName() {
		if (RANDOM_CAMPAIGN.nextInt(100) < 30) {
			return "campaign" + (1 + RANDOM_CAMPAIGN_NAME.nextInt(3));
		}
		return null;
	}
}
