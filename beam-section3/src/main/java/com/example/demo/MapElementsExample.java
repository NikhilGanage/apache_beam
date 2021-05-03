package com.example.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapElementsExample {

	public static void main(String[] args) {
		
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> collection =  pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\customer.csv"));
		
		PCollection<String> pOutput = collection.apply(MapElements.into(TypeDescriptors.strings()).via((String str) -> str.toUpperCase()));
		
		pOutput.apply(TextIO.write().to("E:\\dev\\beam-input\\customer_map_output.csv").withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();

	}

}
