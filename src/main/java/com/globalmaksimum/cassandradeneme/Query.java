package com.globalmaksimum.cassandradeneme;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import me.prettyprint.cassandra.model.MultigetCountQuery;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.CountQuery;

public class Query {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Cluster myCluster = HFactory.getOrCreateCluster("Test Cluster",
				"localhost:9160");
		Keyspace createKeyspace = HFactory.createKeyspace("deneme", myCluster);

		long start = System.currentTimeMillis();
		BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(30);
		ExecutorService executorService = new ThreadPoolExecutor(10, 10, 1,
				TimeUnit.HOURS, workQueue, new CallerRunsPolicy());

		for (int i = 1; i <= 100000; i = i + 10) {
			List<String> keys = new ArrayList<String>(10);
			for (int j = i; j < i + 10; j++) {
				keys.add(String.format("%1$026d", j));
			}
			executorService.execute(new QueryRunnable("global", createKeyspace,
					keys));
			if ((i - 1) % 1000 == 0)
				System.out.println(String.format("current index %1$d", i));
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

	public static class QueryRunnable implements Runnable {
		private String cf;
		private Keyspace keyspace;
		private List<String> keys;

		public QueryRunnable(String cf, Keyspace keyspace, List<String> keys) {
			this.cf = cf;
			this.keyspace = keyspace;
			this.keys = keys;
		}

		@Override
		public void run() {
			for (String key : keys) {
				CountQuery<String, Date> query = createCountQuery(keyspace, cf,
						key);
				query.execute();
				query = createCountQuery(keyspace, "campaign1", key);
				query.execute();
			}
		}

		protected static CountQuery<String, Date> createCountQuery(
				Keyspace createKeyspace, String cf, String key) {
			CountQuery<String, Date> countQuery = HFactory.createCountQuery(
					createKeyspace, StringSerializer.get(),
					DateSerializer.get());
			countQuery.setColumnFamily(cf);
			countQuery.setKey(key);
			Calendar instance = Calendar.getInstance();
			instance.set(2012, 1, 1, 0, 0, 0);
			Date time = instance.getTime();
			countQuery.setRange(time, new Date(time.getTime() + 600000), 3000);
			return countQuery;
		}

	}

}
