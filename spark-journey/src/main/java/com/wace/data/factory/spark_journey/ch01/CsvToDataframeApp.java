package com.wace.data.factory.spark_journey.ch01;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class CsvToDataframeApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CsvToDataframeApp app = new CsvToDataframeApp();
		app.start();
		

	}
	
	private void start() {
		SparkSession spark = SparkSession.builder()
				.appName("CSV to Dataset")
				.master("local")
				.getOrCreate();
		Dataset<Row> df = spark.read().format("csv")
				.option("header", "true")
				.load("data/book.csv");
		
		df.show(5);
						 
	}

}
