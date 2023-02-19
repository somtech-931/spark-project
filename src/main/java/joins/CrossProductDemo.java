package joins;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class CrossProductDemo {

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("CrossProductDemo").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Tuple2<Integer,Integer>> visitsRaw = new ArrayList<>();
    visitsRaw.add(new Tuple2<>(4,18));
    visitsRaw.add(new Tuple2<>(6,4));
    visitsRaw.add(new Tuple2<>(10,9));

    List<Tuple2<Integer,String>> usersRaw = new ArrayList();
    usersRaw.add(new Tuple2<>(1,"John"));
    usersRaw.add(new Tuple2<>(2,"Bob"));
    usersRaw.add(new Tuple2<>(3,"Alan"));
    usersRaw.add(new Tuple2<>(4,"Doris"));
    usersRaw.add(new Tuple2<>(5,"Maybelle"));
    usersRaw.add(new Tuple2<>(6,"Raquel"));

    JavaPairRDD<Integer, Integer> visitsRDD = sc.parallelizePairs(visitsRaw);
    JavaPairRDD<Integer,String> usersRDD = sc.parallelizePairs(usersRaw);

    /*Cross Product or Cartesian Product fof 2 RDDs*/
    JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesian = visitsRDD.cartesian(usersRDD);

    cartesian.collect().forEach(System.out::println);


  }
}
