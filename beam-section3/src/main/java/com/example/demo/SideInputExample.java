package com.example.demo;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class SideInputExample {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();

		PCollection<KV<String,String>> pReturnMap =  pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\return.csv"))
				.apply(ParDo.of(new DoFn<String, KV<String,String>>(){
					
					@ProcessElement
					public void process(ProcessContext context) {
						String arr[] = context.element().split(",");
						context.output(KV.of(arr[0], arr[1]));
					}
				}));
		
		PCollectionView<Map<String, String>> pMapValues =  pReturnMap.apply(View.asMap());
		
		pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\cust_order.csv"))
		.apply(ParDo.of(new DoFn<String, Void>(){
			
			@ProcessElement
			public void process(ProcessContext context) {
				Map<String,String> returnCustDataMap = context.sideInput(pMapValues);
				
				String arr[] = context.element().split(",");
				
				String customerId = returnCustDataMap.get(arr[0]);
				
				if(customerId == null)
					System.out.println(context.element());
			}
		}).withSideInputs(pMapValues));
		
		pipeline.run();
		
	}

}
