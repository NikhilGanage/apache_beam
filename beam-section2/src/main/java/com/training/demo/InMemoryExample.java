package com.training.demo;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class InMemoryExample {

	public static void main(String[] args) {

		Pipeline pipeline = Pipeline.create();
		
		PCollection<CustomerEntity> collection =  pipeline.apply(Create.of(getCustomers()));
		
		PCollection<String> strCollection =  collection.apply(MapElements.into(TypeDescriptors.strings())
												.via((CustomerEntity entity) -> entity.getName()));
		strCollection.apply(TextIO.write().to("E:\\dev\\beam-input\\customer.csv").withNumShards(1).withSuffix(".csv"));
		
		pipeline.run();
		
	}
	
	static List<CustomerEntity> getCustomers(){
		
		CustomerEntity customerEntity = new CustomerEntity(1001, "Adam");
		CustomerEntity customerEntity2 = new CustomerEntity(1002, "John");
		
		List<CustomerEntity> list = new ArrayList<CustomerEntity>();
		list.add(customerEntity);
		list.add(customerEntity2);
		
		return list;
		
	}

}
