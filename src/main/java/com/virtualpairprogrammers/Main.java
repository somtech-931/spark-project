package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

@Slf4j
public class Main {

  public static void main(String[] args) {

    List<Integer> inputData = new ArrayList<>();
    inputData.add(35);
    inputData.add(40);
    inputData.add(58);
    inputData.add(25);
    inputData.add(45);
    inputData.add(78);
    inputData.add(98);

    // 1. Load Spark configuration
    SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
    //2. Get Java Spark Context
    JavaSparkContext sc = new JavaSparkContext(conf);

    //3. Create RDD
    JavaRDD<Integer> myRdd = sc.parallelize(inputData);

    //4. Reduce above RDD
    Integer sumRdd = myRdd.reduce((value1, value2) -> value1 + value2);
    System.out.println("sumRdd: "+sumRdd);

    System.out.println("  ");

    //5. Mapping function
    JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));
    sqrtRdd.foreach(val -> System.out.println("sqrt: "+val));

    sqrtRdd.collect().forEach(System.out::println);

    System.out.println("  ");

    //6. How many elements in sqrtRdd
    System.out.println("count: "+sqrtRdd.count());

    System.out.println("  ");

    //Displaying count with map and reduce
    long singleIntRdd = sqrtRdd.map(value -> 1L).reduce((val1, val2) -> val1 + val2);
    System.out.println("count: "+singleIntRdd);

    // Close Spark context
    sc.close();


  }
}
