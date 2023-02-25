import com.virtualpairprogrammers.Util;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class KeywordRankingEC2 {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("KeywordRankingEC2");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> inputDataRDD = sc.textFile("s3n://som-spark-s3-bucket/input.txt");

    List<Tuple2<Integer, String>> results = inputDataRDD.map(row -> row.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
        .flatMap(row -> Arrays.asList(row.split(" ")).iterator())
        .filter(word -> word.trim().length() > 1 && Util.isNotBoring(word))
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((val1, val2) -> (val1 + val2))
        .mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
        .sortByKey(false)
        .take(10);

    results.forEach(System.out::println);
  }
}
