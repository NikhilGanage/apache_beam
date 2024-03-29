package com.example.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

class MyFilter implements SerializableFunction<String, Boolean>{
	
	@Override
	public  Boolean apply( String input) {
		// TODO Auto-generated method stub
		return input.contains("Los Angeles");
	}
}

public class FilterExample {
	

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		PCollection<String> collection = pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\customer_pardo.csv"));
		
		PCollection<String> pOutput = collection.apply(Filter.by(new MyFilter()));
		
		pOutput.apply(TextIO.write().to("E:\\dev\\beam-input\\customer_pardo_filter_ouput.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();

	}

}
