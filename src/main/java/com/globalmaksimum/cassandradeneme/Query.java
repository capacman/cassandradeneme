package com.globalmaksimum.cassandradeneme;

import java.util.Calendar;
import java.util.Date;

import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.CountQuery;
import me.prettyprint.hector.api.query.QueryResult;

public class Query {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Cluster myCluster = HFactory.getOrCreateCluster("Test Cluster",
				"localhost:9160");
		Keyspace createKeyspace = HFactory.createKeyspace("deneme", myCluster);
		long start = System.currentTimeMillis();
		for (int i = 1; i <= 2000; i++) {
			CountQuery<String, Date> countQuery = createCountQuery(
					createKeyspace, "global", String.format("%1$026d", i));
			QueryResult<Integer> queryResult = countQuery.execute();
		}
		System.out.println(String.format("completed in %1$d",
				System.currentTimeMillis() - start));
	}

	protected static CountQuery<String, Date> createCountQuery(
			Keyspace createKeyspace, String cf, String key) {
		CountQuery<String, Date> countQuery = HFactory.createCountQuery(
				createKeyspace, StringSerializer.get(), DateSerializer.get());
		countQuery.setColumnFamily(cf);
		countQuery.setKey(key);
		Calendar instance = Calendar.getInstance();
		instance.set(2012, 1, 1, 0, 0, 0);
		Date time = instance.getTime();
		countQuery.setRange(time, new Date(time.getTime() + 600000), 3000);
		return countQuery;
	}

}
