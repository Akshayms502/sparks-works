package com.spark.works;

import org.apache.spark.api.java.function.Function;

public class MpgWorks implements Function<String, Integer> {
	int totalMPGCity;
	int totalMPGHwy;

	@Override
	public Integer call(String v1) throws Exception {
		String[] attributeLists=v1.split(",");
		totalMPGCity=Integer.parseInt(attributeLists[9]);
		totalMPGHwy=Integer.parseInt(attributeLists[10]);
		return totalMPGCity;
	}
	
	public double getAverageMPGCity(int count){
		return count;
		
	}

}
