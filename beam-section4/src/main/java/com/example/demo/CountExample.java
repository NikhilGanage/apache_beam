package com.example.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CountExample {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		PCollection<String> pCollection = pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\Count.csv"));

		PCollection<Long> pCount = pCollection.apply(Count.<String>globally());
		
		pCount.apply(ParDo.of(new DoFn<Long, Void>(){
			
			@ProcessElement
			public void process(ProcessContext context) {
				System.out.println(context.element());
			}
			
		}));
		
		pipeline.run();

	}

}
