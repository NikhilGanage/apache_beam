package com.example.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;

public class DistinctExample {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		PCollection<String> pCollection = pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\Distinct.csv"));

		PCollection<String> uniqueCollection = pCollection.apply(Distinct.<String>create());
		
		uniqueCollection.apply(TextIO.write().to("E:\\dev\\beam-input\\distinct_output.csv")
				.withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();
		
	}

}
