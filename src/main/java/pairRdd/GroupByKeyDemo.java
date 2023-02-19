package pairRdd;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

public class GroupByKeyDemo {

  public static void main(String[] args) {

    /*Input Data*/
    List<String> inputData = new ArrayList<>();
    inputData.add("WARN: Monday 7th April");
    inputData.add("ERROR: Tuesday 8th April");
    inputData.add("FATAL: Tuesday 8th April");
    inputData.add("ERROR: Wednesday 9th Ma");
    inputData.add("WARN: Friday 10h April");

    JavaSparkContext sc =  new JavaSparkContext(new SparkConf().setAppName("groupByKeyDemo").setMaster("local[*]"));
    JavaRDD<String> originalLogRDD = sc.parallelize(inputData);

    originalLogRDD.mapToPair(originalMsg -> new Tuple2<>(originalMsg.split(":")[0],1))
                  .groupByKey()
                  .collect()
                  .forEach(tuple -> System.out.println(tuple._1 + " has "+ Iterables.size(tuple._2) + " messages."));
  }
}
