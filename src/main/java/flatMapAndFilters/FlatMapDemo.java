package flatMapAndFilters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FlatMapDemo {

  public static void main(String[] args) {

    /*Input Data*/
    List<String> inputData = new ArrayList<>();
    inputData.add("WARN: Monday 7th April");
    inputData.add("ERROR: Tuesday 8th April");
    inputData.add("FATAL: Tuesday 8th April");
    inputData.add("ERROR: Wednesday 9th Ma");
    inputData.add("WARN: Friday 10h April");

    SparkConf conf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> inputRdd = sc.parallelize(inputData);

    /*Requirement: We want to list down all words in the inputData RDD*/
    JavaRDD<String> wordsRDD = inputRdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
    wordsRDD.collect().forEach(System.out::println);

  }
}
