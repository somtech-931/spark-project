package assignments;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class BigDataExerciseRDD {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("ReadingFilesDemo").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    sc.textFile("");
  }

}
