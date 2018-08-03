package com.spark.works;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.spark.commons.SparkConnection;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkSqlDemo {
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR); 
		Logger.getLogger("aka").setLevel(Level.ERROR);
		
		JavaSparkContext sparkContext=SparkConnection.getContext();
		SparkSession spSession= SparkConnection.getSession();
		
		Dataset<Row> empDataFields=spSession.read().json("./data/customer.json");
		empDataFields.show();
		empDataFields.printSchema();
		
		
		
		
		empDataFields.select(col("name"),col("salary")).show();
		
		
		empDataFields.filter(col("gender").equalTo("male")).show();
		
		
		empDataFields.groupBy(col("gender")).count().show();
		
		
		Dataset<Row> empData=empDataFields.groupBy(col("deptid")).agg(avg(empDataFields.col("salary")), max(empDataFields.col("age")));
		empData.show();
		
		
		Department dept1=new Department(100,"development");
		Department dept2=new Department(200,"testing");
		
		List<Department> deptList=new ArrayList<Department>();
		deptList.add(dept1);
		deptList.add(dept2);
		
		Dataset<Row> deptDataFields=spSession.createDataFrame(deptList, Department.class);
		System.out.println("depatment contents are");
		deptDataFields.show();
		
		
		Dataset<Row> empDeptJoin=empDataFields.join(deptDataFields);
		empDeptJoin.show();
		
		Dataset<Row> empDeptJoin1=empDataFields.join(deptDataFields,col("deptid").equalTo(col("departmentId")));
		empDeptJoin1.show();
		
		empDataFields.filter(col("age").gt(30)).join(deptDataFields,col("deptid").equalTo(col("departmentId")))
		.groupBy(col("deptid")).agg(avg(empDataFields.col("salary")),max(empDataFields.col("age"))).show();
		
		
		System.out.println("---------------------------------------------------------------");
		
		Dataset<Row> autoData=spSession.read().option("header", "true").csv("./data/auto-data.csv");
		autoData.show(5);
		
		//creating RDD with row object
		Row row1=RowFactory.create(1,"India","bengaluru");
		Row row2=RowFactory.create(1,"USA","Restron");
		Row row3=RowFactory.create(3,"UK","SteevenScreek");
		List<Row> rList=new ArrayList<Row>();
		rList.add(row1);
		rList.add(row2);
		rList.add(row3);
		
		JavaRDD<Row> rowRDD=sparkContext.parallelize(rList);
		StructType schema=DataTypes.createStructType(new StructField[]{
				DataTypes.createStructField("id", DataTypes.IntegerType, false),
				DataTypes.createStructField("country", DataTypes.StringType, false),
				DataTypes.createStructField("city", DataTypes.StringType, false)
				
			});
		
		Dataset<Row> tempDataFields=spSession.createDataFrame(rowRDD, schema);
		tempDataFields.show();
		
		System.out.println("---------------------------------------------------------");
		//working with csv data with sql statements
		
		autoData.createOrReplaceTempView("autos");
		System.out.println("temp table contents");
		
		spSession.sql("select * from autos where hp>200").show();
		
		spSession.sql("select make,max(rpm) from autos group by make order by 2").show();
		
		//covert DataFrame to javaRdd
		JavaRDD<Row> autoRDD=autoData.rdd().toJavaRDD();
		
		//reading data from mysql
		
		Map<String,String> jdbcConnectionParams=new HashMap<String,String>();
		jdbcConnectionParams.put("url", "jdbc:mysql://localhost:3306/spring");
		jdbcConnectionParams.put("driver","com.mysql.jdbc.Driver");
		jdbcConnectionParams.put("dbtable", "employee");
		jdbcConnectionParams.put("user", "root");
		jdbcConnectionParams.put("password", "root@123");
		
		Dataset<Row> sqlDataFields=spSession.read().format("jdbc").options(jdbcConnectionParams).load();
		
		sqlDataFields.show();
		
	}

}
