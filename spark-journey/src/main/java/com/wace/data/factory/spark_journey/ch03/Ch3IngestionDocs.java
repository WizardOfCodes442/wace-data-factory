package com.wace.data.factory.spark_journey.ch03;

import org.apache.spark.Partition;

public class Ch3IngestionDocs {

	//Partion are created, and data is assigned to each partion 
	//automatically based on your infrastructure (number of nodes and
	// size of the dataset
	//you can findout how many partition you have with the following code 
	
	System.out.println("**** Looking at partitions");
	Partition[] partitions = df.rdd().partitions();
	int partitionCount  = partitions.length;
	System.out.println("Partition count before repartition: " + partitionCount);
	
}
