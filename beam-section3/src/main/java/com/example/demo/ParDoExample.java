package com.example.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

class CustFilter extends DoFn<String, String>{
	
	@ProcessElement
	public void processElement(ProcessContext context) {
		String line = context.element();
		
		String arr[] = line.split(",");
		
		if(arr[3].equals("Los Angeles")) {
			context.output(line);
		}
	}
}

public class ParDoExample {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		PCollection<String> collection = pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\customer_pardo.csv"));
		
		PCollection<String> pOutput = collection.apply(ParDo.of(new CustFilter()));
		
		pOutput.apply(TextIO.write().to("E:\\dev\\beam-input\\customer_pardo_ouput.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();

	}

}
