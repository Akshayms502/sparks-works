package com.spark.commons;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DataSource {
	
	public static JavaRDD<Integer> getCollData(){
		JavaSparkContext spContext=SparkConnection.getContext();
		List<Integer> data=Arrays.asList(3,54,67,134,64,767,24,65);
		JavaRDD<Integer> collData=spContext.parallelize(data);
		
		collData.cache();
		return collData;
	}

}
