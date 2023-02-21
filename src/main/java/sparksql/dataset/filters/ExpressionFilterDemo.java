package sparksql.dataset.filters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ExpressionFilterDemo {

  public static void main(String[] args) {

    /*Create SparksSession*/
    SparkSession session = SparkSession.builder().appName("DatasetExpressionFilterDemo").master("local[*]").getOrCreate();

    /*Read input data file*/
    Dataset<Row> studentsDataset = session.read()
        .option("header",true)    // required if we want to do operations based on header keys
        .csv("src/main/resources/exams/students.csv"); //relative path to data file

    /*Filter students dataset based on condition:
    * 1.subject = "Modern art"
    * 2.year > 2007*/
    Dataset<Row> modernArtStuds = studentsDataset.filter("subject = 'Modern Art' AND year >= 2007");

    /*Show results of the filtered Dataset
    * This will show only the 1st 20 records.*/
    modernArtStuds.show();

    System.out.println("");

    /*Count of students in the raw data who had modern art in their curriculum.*/
    System.out.println("Number of students opting modern art: "+modernArtStuds.count());
  }
}
