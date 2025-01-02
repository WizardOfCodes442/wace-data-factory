package com.wace.data.factory.spark_journey.ch02;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.collect_list;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
public class CsvToRelationalDatabaseApp {
	public static void main(String[]  args) {
		CsvToRelationalDatabaseApp  app = new CsvToRelationalDatabaseApp();
	} 
	
	private void start() {
		SparkSession spark = SparkSession.builder()
				.appName("CSV to DB")
				.master("local")
				.getOrCreate();
		Dataset<Row> df = spark.read()
				`format("csv")
				.option("header", "true")
				.load("data/authors.csv");
		df = df.withColumn(
				"name",
				concat(df.col("lname"),
				lit(", "), df.col("fname")));
		String dbConnectionUrl =
				"jdbc:postgresql://localhost/spark_labs";
		Properties prop = new Properties();
		prop.setProperty("driver", "org.postgresql.Driver");
		prop.setProperty("user", "jgp");
		prop.setProperty("password", "Spark<3Java");
		df.write()
			.mode(SaveMode.Overwrite)
			.jdbc(dbConnectionUrl, "ch02", prop);
		System.out.println("Process complete");
			
					
	}


}