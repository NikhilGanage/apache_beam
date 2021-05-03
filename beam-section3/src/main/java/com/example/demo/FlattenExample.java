package com.example.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenExample {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		PCollection<String> pCustomerList1 = pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\customer_1.csv"));
		
		PCollection<String> pCustomerList2 = pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\customer_2.csv"));
		
		PCollection<String> pCustomerList3 = pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\customer_3.csv"));

		PCollectionList<String> pMergedCustomerList = PCollectionList.of(pCustomerList1).and(pCustomerList2).and(pCustomerList3);
		
		PCollection<String> pFlattenCustomerList = pMergedCustomerList.apply(Flatten.pCollections());
		
		pFlattenCustomerList.apply(TextIO.write().to("E:\\dev\\beam-input\\customer_flatten_output.csv")
				.withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();
		
	}

}
