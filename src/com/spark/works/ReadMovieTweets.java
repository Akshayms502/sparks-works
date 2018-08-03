package com.spark.works;

import org.apache.avro.file.SyncableFileOutputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadMovieTweets {

	public static void main(String[] args) {
			String appName="sampleApp";
			String sparkMaster="local[2]";
			
			JavaSparkContext sparkContext=null;
			SparkConf conf=new SparkConf().setAppName(appName).setMaster(sparkMaster);

			sparkContext=new JavaSparkContext(conf);
			
			JavaRDD<String> tweetsRDD=sparkContext.textFile("./data/movietweet.csv");
			
			
			tweetsRDD.take(5).forEach(System.out::println);
			int count=(int) tweetsRDD.count();
			
			System.out.println("number of tweets are"+count);
			
			JavaRDD<String> upperCaseRDD=tweetsRDD.map(temp->temp.toUpperCase());
			
			upperCaseRDD.take(10).forEach(System.out::println);
			
			while(true){
				try{
					Thread.sleep(100);
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			
	}

}
