package com.spark.commons;

import org.apache.spark.api.java.JavaRDD;

public class Utilities {
	
	public static void printStringRDD(
		JavaRDD<String> stringRDD,int limit){
		for(String temp:stringRDD.take(limit)){
			System.out.println(temp);
		}
		System.out.println("---------------------------");
	}
	
	public static void hold(){
		while(true){
			try{
				Thread.sleep(1000);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	

}
