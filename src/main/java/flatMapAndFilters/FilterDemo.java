package flatMapAndFilters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;

public class FilterDemo {

  public static void main(String[] args) {

    /*Input Data*/
    List<String> inputData = new ArrayList<>();
    inputData.add("WARN: Monday 7th April");
    inputData.add("ERROR: Tuesday 8th April");
    inputData.add("FATAL: Tuesday 8th April");
    inputData.add("ERROR: Wednesday 9th Ma");
    inputData.add("WARN: Friday 10h April");

    SparkConf conf = new SparkConf().setAppName("FilterDemo").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> inputRDD = sc.parallelize(inputData);

    /*Requirement: We want to list down all words in list RDD except the ones ending with :
    * i.e. except the logging levels*/
    JavaRDD<String> outputRdd = inputRDD.flatMap(input -> Arrays.asList(input.split(" ")).iterator())
        .filter(word -> !word.endsWith(":"));
    outputRdd.collect().forEach(System.out::println);
  }
}
