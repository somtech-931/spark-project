package joins;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class InnerJoinDemo {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("InnerJoinDemo").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Tuple2<Integer,Integer>> visitsRaw = new ArrayList<>();
    visitsRaw.add(new Tuple2<>(4,18));
    visitsRaw.add(new Tuple2<>(6,4));
    visitsRaw.add(new Tuple2<>(10,9));

    List<Tuple2<Integer,String>> usersRaw = new ArrayList<>();
    usersRaw.add(new Tuple2<>(1, "John"));
    usersRaw.add(new Tuple2<>(2, "Bob"));
    usersRaw.add(new Tuple2<>(3, "Alan"));
    usersRaw.add(new Tuple2<>(4, "Doris"));
    usersRaw.add(new Tuple2<>(5, "Marybelle"));
    usersRaw.add(new Tuple2<>(6, "Raquel"));

    /*Create users PairRDD*/
    JavaPairRDD<Integer, String> usersPairRDD = sc.parallelizePairs(usersRaw);

    /*Create visits PairRDD*/
    JavaPairRDD<Integer, Integer> visitsPairRDD = sc.parallelizePairs(visitsRaw);

    /*INNER JOIN*/
    JavaPairRDD<Integer, Tuple2<Integer,String>> innerJoinRdd = visitsPairRDD.join(usersPairRDD);
    innerJoinRdd.collect().forEach(tuple -> System.out.println(tuple._2._2 + " has made "+ tuple._1 + " visits."));
  }
}
