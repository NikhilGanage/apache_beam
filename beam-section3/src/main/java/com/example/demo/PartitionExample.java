package com.example.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

class MyPartition implements PartitionFn<String>{

	@Override
	public int partitionFor(String elem, int numPartitions) {
		
		String arr[] = elem.split(",");
		
		if(arr[3].equals("Los Angeles"))		
			return 0;
		else if(arr[3].equals("Phoenix"))
			return 1;
		else
			return 2;
	}
	
}

public class PartitionExample {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		PCollection<String> pCollection = pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\Partition.csv"));
		
		PCollectionList<String> collectionList =  pCollection.apply(Partition.of(3, new MyPartition()));
		
		PCollection<String> p0 = collectionList.get(0);
		PCollection<String> p1 = collectionList.get(1);
		PCollection<String> p2 = collectionList.get(2);
		
		p0.apply(TextIO.write().to("E:\\dev\\beam-input\\p0.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
		p1.apply(TextIO.write().to("E:\\dev\\beam-input\\p1.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
		p2.apply(TextIO.write().to("E:\\dev\\beam-input\\p2.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();
		
	}

}
