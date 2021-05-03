package com.example.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class User extends SimpleFunction<String, String>{

	@Override
	public String apply(String input) {
		
		String[] arr = input.split(",");
		
		String sessionId = arr[0];
		String userId = arr[1];
		String userName = arr[2];
		String videoId = arr[3];
		String duration = arr[4];
		String startTime = arr[5];
		String sex = arr[6];
		
		String output = "";
		if(sex.equals("1")) {
			output = sessionId+","+userId+","+userName+","+videoId+","+duration+","+startTime+","+sex+","+"M";
		}
		else if(sex.equals("2")){
			output = sessionId+","+userId+","+userName+","+videoId+","+duration+","+startTime+","+sex+","+"F";
		}
		else {
			output = input;
		}
		
		return output;
	}
	
}

public class MapElementsSimpleFunction {

	public static void main(String[] args) {
		Pipeline pipeline = Pipeline.create();
		
		PCollection<String> collection =  pipeline.apply(TextIO.read().from("E:\\dev\\beam-input\\user.csv"));
		
		PCollection<String> strCollection = collection.apply(MapElements.via(new User()));
		
		strCollection.apply(TextIO.write().to("E:\\dev\\beam-input\\user_function_output.csv").withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();

	}

}
