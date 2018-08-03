package com.spark.works;

import java.util.Arrays;

import org.apache.spark.api.java.function.Function;

public class ClenseRidCars implements Function<String, String> {

	@Override
	public String call(String v1) throws Exception {
		String[] attributeLists=v1.split(",");
		attributeLists[3]=(attributeLists[3].equals("two"))?"2":"4";
		attributeLists[1]=attributeLists[4].toUpperCase();
		return Arrays.toString(attributeLists);
	}

}
