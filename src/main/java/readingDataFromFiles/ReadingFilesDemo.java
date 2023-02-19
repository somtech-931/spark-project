package readingDataFromFiles;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadingFilesDemo {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("ReadingFilesDemo").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> inputRDD = sc.textFile("src/main/resources/subtitles/input.txt");
    inputRDD.flatMap(input -> Arrays.asList(input.split(" ")).iterator()).collect().forEach(System.out::println);
  }
}
