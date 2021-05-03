package com.training.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class LocalFileExample {

	public static void main(String[] args) {
		
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> collection =  pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\input.csv"));
		
		collection.apply(TextIO.write().to("E:\\dev\\beam-input\\output.csv").withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();
	}

}
