import com.virtualpairprogrammers.Util;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class KeywordsProgram {

  public static void main(String[] args) {

    JavaRDD<String> inputRDD = new JavaSparkContext(new SparkConf().setAppName("keywords").setMaster("local[*]"))
                              .textFile("src/main/resources/subtitles/input.txt");

    inputRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]","").toLowerCase())
            .filter(sentence->sentence.trim().length()>0)
            .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
            .filter(word ->  word.trim().length() > 0)
            .filter(word -> Util.isNotBoring(word))
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((value1,value2)->(value1+value2))
            .mapToPair(tuple -> new Tuple2<>(tuple._2,tuple._1))
            .sortByKey(false)
            .collect()
            .forEach(System.out::println);
  }
}
