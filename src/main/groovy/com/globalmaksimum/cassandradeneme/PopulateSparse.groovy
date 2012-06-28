package com.globalmaksimum.cassandradeneme;
import java.util.Date;

import me.prettyprint.cassandra.serializers.AsciiSerializer;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DateSerializer;
import me.prettyprint.hector.api.beans.HColumn;



import java.util.Random;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;


public class PopulateSparse {
	def static main(String[] args){
		def drop=args.any { it.equalsIgnoreCase("drop") }
		println "drop data ${drop}"
		def myCluster = HFactory.getOrCreateCluster("Test Cluster","localhost:9160")
		def keySpace=Populate.getKeySpace(myCluster, drop)
		Calendar calendar = Calendar.getInstance()
		calendar.set(2012, 1, 1, 0, 0, 0)
		def time=calendar.getTime()
		Mutator<String> mutator = HFactory.createMutator(keySpace,new AsciiSerializer())
		(0..60).each {
			(0..600000).each {
				if(mutator.pendingMutationCount>300){
					mutator.execute()
					println "mutator executed"
					mutator=HFactory.createMutator(keySpace,new AsciiSerializer())
				}
				def customer=String.format('%1$026d', it)
				HColumn<Date, byte[]> column = HFactory.createColumn(time,[]as byte[], DateSerializer.get(),BytesArraySerializer.get())
				String hitCampaignName = Populate.getHitCampaignName()
				if (hitCampaignName != null)
					mutator.addInsertion(customer, hitCampaignName, column)
				mutator.addInsertion(customer, "global", column)
			}
			time=new Date(time.getTime()+30000)
			println "iterator ${it}"
		}
	}
}
