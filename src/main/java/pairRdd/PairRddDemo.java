package pairRdd;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class PairRddDemo {

  public static void main(String[] args) {

    /*Input Data*/
    List<String> inputData = new ArrayList<>();
    inputData.add("WARN: Monday 7th April");
    inputData.add("ERROR: Tuesday 8th April");
    inputData.add("FATAL: Tuesday 8th April");
    inputData.add("ERROR: Wednesday 9th Ma");
    inputData.add("WARN: Friday 10h April");

    /*Initialize Spark*/
    SparkConf conf = new SparkConf().setAppName("PairRddDemo").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    /*Create RDD of log messages*/
    JavaRDD<String> originalLogRdd = sc.parallelize(inputData);

    /*Intention: Now we want to count the messages as per their log levels
    * Like a Key value pair
    * WARN: 2
    * ERROR:2
    * FATAL:1*/

    JavaPairRDD<String, String> pairRDD = originalLogRdd.mapToPair(originalLog -> {
      String logLevel = originalLog.split(":")[0];
      String logText = originalLog.split(":")[1];
      return new Tuple2<>(logLevel, logText);
    });
    pairRDD.collect().forEach(tuple -> System.out.println(tuple._1 + " : " + tuple._2));
    System.out.println("");

    JavaPairRDD<String, Integer> pairRddForSum = originalLogRdd.mapToPair(originalLog -> {
      String logLevel = originalLog.split(":")[0];
      return new Tuple2<>(logLevel, 1);
    });

    JavaPairRDD<String, Integer> sumRDD =
        pairRddForSum.reduceByKey((value1, value2) -> value1 + value2);
    sumRDD.collect().forEach(tuple -> System.out.println(tuple._1 + " has " + tuple._2+" occurrences"));

    System.out.println("");

  }
}
