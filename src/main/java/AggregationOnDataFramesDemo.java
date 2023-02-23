import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

public class AggregationOnDataFramesDemo {

  public static void main(String[] args) {

    /*1. Create Spark session*/
    SparkSession session = SparkSession.builder().appName("AggregationOnDataFramesDemo").master("local[*]").getOrCreate();

    /*2. Read big data students file*/
    Dataset<Row> studentsRawData = session.read().option("header", true).csv("src/main/resources/exams/students.csv");
    //studentsRawData.show();

    /*3.Get maximum by using agg() function first*/
    Dataset<Row> maxScoreBySubject = studentsRawData.groupBy("subject").agg(max(col("score")).cast(DataTypes.IntegerType).alias("MAX_SCORE"));
    maxScoreBySubject.show();

    System.out.println("");

    /*4.Get min by each subject*/
    Dataset<Row> minScoreBySubject =
        studentsRawData.groupBy("subject").agg(min(col("score")).cast(DataTypes.IntegerType).alias("MIN_SCORE"));
    minScoreBySubject.show();

    System.out.println("");

    /*5. Get avg score for each subject*/
    Dataset<Row> avgScoreBySubject =
        studentsRawData.groupBy("subject").agg(avg(col("score")).cast(DataTypes.IntegerType).alias("avgScore"));
    avgScoreBySubject.show();
    /*Dataset<Row> studDataSchemaInferred = session.read().option("header", true).option("inferSchema", true).csv("src/main/resources/exams/students.csv");
    Dataset<Row> maxScoreBySubject = studDataSchemaInferred.groupBy("subject").max("score");
    maxScoreBySubject.show();*/

    session.close();
  }
}
