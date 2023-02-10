package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class TupleDemo {

  public static void main(String[] args) {

    List<Integer> inputData = new ArrayList<>();
    inputData.add(45);
    inputData.add(25);
    inputData.add(9);
    inputData.add(2);
    inputData.add(144);

    SparkConf conf = new SparkConf().setAppName("Tuple Demo").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    //Create RDD
    JavaRDD<Integer> integersRDD = sc.parallelize(inputData);

    //Reduce : Want to add all these integers
    Integer sumRDD = integersRDD.reduce((value1, value2) -> value1 + value2);
    System.out.println("sumRDD: " + sumRDD.toString());

    System.out.println("  ");
    //Map --> Want to find sqrt of these integers
    JavaRDD<Double> sqrtRdd = integersRDD.map(val -> Math.sqrt(val));
    sqrtRdd.collect().forEach(System.out::println);

    System.out.println("  ");
    //Tuple Begins
    //Tuple2 Demo
    // Req: --> Say we want o/p like: val,sqrt(val) eg: (25,5)
    JavaRDD<Tuple2<Integer, Double>> tupleValSqrt = integersRDD.map(value -> new Tuple2<>(value, Math.sqrt(value)));
    tupleValSqrt.collect().stream().forEach(System.out::println);

    System.out.println("  ");

    //Tuple3 Demo
    // Req: --> Say we want o/p like: val,sqrt(val),val+sqrt(val) eg: (25,5,30)
    JavaRDD<Tuple3> tuple3DemoRdd = integersRDD.map(value -> new Tuple3(value, Math.sqrt(value), value + Math.sqrt(value)));
    tuple3DemoRdd.collect().stream().forEach(System.out::println);
  }
}
