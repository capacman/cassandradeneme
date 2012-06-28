package com.globalmaksimum.cassandradeneme
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

import me.prettyprint.cassandra.model.MultigetCountQuery;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory
import me.prettyprint.hector.api.query.CountQuery;

class QuerySparse {
	private static Random RANDOM_CUSTOMER = new Random(System.currentTimeMillis())
	def static main(String[] args){
		def myCluster = HFactory.getOrCreateCluster("Test Cluster","localhost:9160")
		def keySpace=Populate.getKeySpace(myCluster, false)
		long start = System.currentTimeMillis();
		BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(30)
		ExecutorService executorService = new ThreadPoolExecutor(10, 10, 1,TimeUnit.HOURS, workQueue, new CallerRunsPolicy())
		for (int i = 0; i < 600000; i = i + 10) {
			List<String> keys = new ArrayList<String>(10)
			for (int j = i; j < i + 10; j++) {
				keys.add(String.format('%1$026d', RANDOM_CUSTOMER.nextInt(600000)))
			}
			executorService.execute(new QueryRunnable("global", keySpace,keys))
			if ((i - 1) % 1000 == 0)
				System.out.println(String.format('current index %1$d', i))
		}
		executorService.shutdown()
		try {
			executorService.awaitTermination(3, TimeUnit.MINUTES)
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace()
		}
		System.out.println(String.format('completed in %1$d',System.currentTimeMillis() - start))
		HFactory.shutdownCluster(myCluster)
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
			MultigetCountQuery<String, Date> query = createCountQuery(keyspace, cf,
					keys.toArray());
			query.execute();
			query = createCountQuery(keyspace, "campaign1", keys.toArray());
			query.execute();
		}

		protected static MultigetCountQuery<String, Date> createCountQuery(
		Keyspace createKeyspace, String cf, String[] keys) {
			MultigetCountQuery<String, Date> cq = new MultigetCountQuery<String, Date>(createKeyspace, StringSerializer.get(),
					DateSerializer.get())
			cq.setColumnFamily(cf);
			cq.setKeys(keys)
			Calendar instance = Calendar.getInstance();
			instance.set(2012, 1, 1, 0, 0, 0);
			Date time = instance.getTime();
			cq.setRange(time, new Date(time.getTime() + 600000), 3000);
			return cq
		}
	}
}
