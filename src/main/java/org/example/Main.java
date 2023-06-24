package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.desc;

public class Main {
    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir", "C:\\MyWork\\Lesson\\hadoop-common-2.2.0-bin-master");

//        SparkSession spark = SparkSession.builder().appName("Simple Application").config("dfs.client.use.datanode.hostname", true).config("spark.master", "local").getOrCreate();
//        Dataset<Row> breweries = spark.read().format("csv").option("header", true).load("hdfs://localhost:9000/test/breweries.csv");
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<Row> breweries = spark.read().format("csv").option("header", true).load("hdfs://namenode:9000/test/breweries.csv");
        Dataset<Row> groupStates = breweries.groupBy("state").count();
        List<Row> sortedGroupStates = groupStates.orderBy(desc("count")).takeAsList(10);
        for (Row sortedGroupState : sortedGroupStates) {
            System.out.println(sortedGroupState.getString(0) + " : " + sortedGroupState.getLong(1));
        }
    }
}