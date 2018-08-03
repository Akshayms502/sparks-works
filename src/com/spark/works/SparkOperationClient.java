package com.spark.works;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.spark.commons.DataSource;
import com.spark.commons.SparkConnection;
import com.spark.commons.Utilities;

public class SparkOperationClient {
	public static void main(String[] args) {
	Logger.getLogger("org").setLevel(Level.ERROR); 
	Logger.getLogger("aka").setLevel(Level.ERROR);
	
	JavaSparkContext sparkContext=SparkConnection.getContext();
	
	JavaRDD<Integer> collData=DataSource.getCollData();
	System.out.println("total no of records"+collData.count());
	
	JavaRDD<String> autoDataContent=sparkContext.textFile("./data/auto-data.csv");
	System.out.println("number of records"+autoDataContent.count());
	autoDataContent.take(5).forEach(System.out::println);
	
	System.out.println("------------------------------------------");
	Utilities.printStringRDD(autoDataContent,10);
	
//	autoDataContent.saveAsTextFile("./data/modified.csv");
	System.out.println("------------------------------------------------------------------");
	
	JavaRDD<String> tsvData=autoDataContent.map(str->str.replace(",", "\t"));
	Utilities.printStringRDD(tsvData, 5);
	
	System.out.println("----------------------------------------------------------------------");

	String header=autoDataContent.first();
	JavaRDD<String> autoDataWithoutHeader=autoDataContent.filter(s->!s.equals(header));
	Utilities.printStringRDD(autoDataWithoutHeader, 5);
	
	System.out.println("------------------------------------------------");
	
	JavaRDD<String> carNameData=autoDataContent.filter(str->str.contains("toyota"));
	Utilities.printStringRDD(carNameData, 10);
	
	System.out.println("---------------------------------------------");
	
	JavaRDD<String> duplicate=autoDataContent.distinct();
	Utilities.printStringRDD(duplicate, 5);
	
	System.out.println("----------------------------------------------------------");
	
	JavaRDD<String> words=carNameData.flatMap(new FlatMapFunction<String, String>() {

		@Override
		public Iterator<String> call(String t) throws Exception {	
			return Arrays.asList(t.split(",")).iterator();
		}
	});
	
	System.out.println("Toyota word count"+words.count());
	System.out.println("-------------------------------------------------------------");
	
	JavaRDD<String> cleanseRDD=autoDataContent.map(new ClenseRidCars());
	Utilities.printStringRDD(cleanseRDD, 5);
	
	System.out.println("--------------------------------------------------------------");
	
	JavaRDD<String> words1=sparkContext.parallelize(Arrays.asList("hello","how","are","you","today"));
	JavaRDD<String> words2=sparkContext.parallelize(Arrays.asList("hello","all","banglaorean"));
	
	System.out.println("union operation-set");
	Utilities.printStringRDD(words1.union(words2), 10);
	
	System.out.println("intersection operation");
	Utilities.printStringRDD(words1.intersection(words2), 10);
	
	System.out.println("-----------------------------------------");
	
	Integer collDataCount=collData.reduce((x,y)->x+y);
	System.out.println("sum of given integer"+collDataCount);
	
	System.out.println("------------------------------------------------------------------");
	
	
	
	
	
	}
}	
	
