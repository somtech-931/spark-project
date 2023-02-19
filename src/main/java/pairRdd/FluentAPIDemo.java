package pairRdd;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class FluentAPIDemo {

  public static void main(String[] args) {

      /*Input Data*/
      List<String> inputData = new ArrayList<>();
      inputData.add("WARN: Monday 7th April");
      inputData.add("ERROR: Tuesday 8th April");
      inputData.add("FATAL: Tuesday 8th April");
      inputData.add("ERROR: Wednesday 9th Ma");
      inputData.add("WARN: Friday 10h April");

      SparkConf conf = new SparkConf().setAppName("PairRddDemo").setMaster("local[*]");
      JavaSparkContext sc = new JavaSparkContext(conf);

      JavaRDD<String> originalLogRdd =  sc.parallelize(inputData);

      originalLogRdd.mapToPair(originalLogMsg -> new Tuple2<>(originalLogMsg.split(":")[0],1))
          .reduceByKey((val1,val2)-> val1+val2)
          .collect().forEach(System.out::println);

  }
}
