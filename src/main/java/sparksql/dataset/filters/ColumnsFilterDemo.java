package sparksql.dataset.filters;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ColumnsFilterDemo {

  public static void main(String[] args) {

    /*Create SparksSession*/
    SparkSession session = SparkSession.builder().appName("ColumnsFilterDemo").master("local[*]").getOrCreate();

    /*Read input data file*/
    Dataset<Row> studentsDataset = session.read().option("header", true).csv("src/main/resources/exams/students.csv");

    /*Extracting columns on which we will filter*/
    Column subjectColumn = studentsDataset.col("subject");
    Column yearColumn = studentsDataset.col("year");

    /*Filter students dataset based on condition:
     * 1.subject = "Modern art"
     * 2.year > 2007
     * Like RDDs Datasets are also immutable, so after filtering we need to hold it in a separate Dataset.*/
    /*Dataset<Row> modernArtStuds = studentsDataset.filter(subjectColumn.equalTo("Modern Art")
        .and(yearColumn.geq(2007)));*/


    /**
     * More modern and used syntax.
     * functions is a Java class from Spark API. We have statically imported the members of function.
     * It provides these static convenient methods like col etc to do manipulation etc
     */
    Dataset<Row> modernArtStuds = studentsDataset.filter(col("subject").equalTo("Modern Art").and(yearColumn.geq(2007)));

    /*Show results of the filtered Dataset
     * This will show only the 1st 20 records.*/
    modernArtStuds.show();

    System.out.println("");

    /*Count of students in the raw data who had modern art in their curriculum.*/
    System.out.println("Number of students opting modern art in 2007: "+modernArtStuds.count());

      session.close();

  }
}
