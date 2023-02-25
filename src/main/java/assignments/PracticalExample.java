package assignments;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.stddev;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PracticalExample {

  public static void main(String[] args) {

    SparkSession session = SparkSession.builder().appName("PracticalExample").master("local[*]").getOrCreate();
    Dataset<Row> studentsRawData = session.read().option("header", true).csv("src/main/resources/exams/students.csv");
    Dataset<Row> filteredDS = studentsRawData.drop("student_id","exam_center_id","quarter","grade");
    //filteredDS.show();
    System.out.println("  ");

    /*Average dataset singly*/
    Dataset<Row> avgDS = filteredDS.groupBy(col("subject"))
              .pivot(String.valueOf(col("year")))
              .agg(round(avg(col("score")),2).alias("Average"))
              .orderBy(col("subject"));
    //avgDS.show();
    System.out.println("  ");

    /*Standard Deviation dataset singly*/
    Dataset<Row> stddevDS = filteredDS.groupBy(col("subject"))
        .pivot(String.valueOf(col("year")))
        .agg(round(stddev(col("score")),2).alias("Standard Deviation"))
        .orderBy(col("subject"));
    //stddevDS.show();

    System.out.println("  ");

    /*Round up results in single table*/
    Dataset<Row> resultsDS = filteredDS.groupBy(col("subject"))
        .pivot(String.valueOf(col("year")))
        .agg(round(avg(col("score")),2).alias("Average"),
             round(stddev(col("score")),2).alias("Standard Deviation"))
        .orderBy(col("subject"));
    resultsDS.show();
  }
}
