package com.example.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

class StringToKV extends DoFn<String, KV<String,Integer>>{
	
	@ProcessElement
	public void process(ProcessContext context) {
		String inputValue = context.element();
		String arr[] = inputValue.split(",");
		context.output(KV.of(arr[0], Integer.valueOf(arr[3])));
	}
}

class KVToString extends DoFn<KV<String,Iterable<Integer>>, String>{
	
	@ProcessElement
	public void process(ProcessContext context) {
		Integer sum = 0;
		for(Integer value : context.element().getValue())
			sum += value;
		
		context.output(context.element().getKey()+","+sum.toString());
	}
}

public class GroupByExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Pipeline pipeline = Pipeline.create();

		pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\GroupByKey_data.csv"))
			.apply(ParDo.of(new StringToKV()))
				.apply(GroupByKey.<String, Integer>create())
					.apply(ParDo.of(new KVToString()))
						.apply(TextIO.write().to("E:\\dev\\beam-input\\group_by_output.csv")
								.withHeader("Id,Amount").withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();
	}

}
