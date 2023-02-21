package sparksql.dataset.basics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BasicDataSetDemo {

  public static void main(String[] args) {

    /*Create SparksSession*/
    SparkSession session = SparkSession.builder().appName("BasicDataSetDemo").master("local[*]").getOrCreate();

    /*Read input data file*/
    Dataset<Row> studentsData = session.read()
        .option("header", true) // required if we want to do operations based on header keys and also read headers
        .csv("src/main/resources/exams/students.csv");  //relative path to data file

    /*Show results of the Dataset
     * This will show only the 1st 20 records.*/
    studentsData.show();

    System.out.println("");

    /*Count of students in the raw data.*/
    System.out.println("Number of students opting modern art: "+studentsData.count());
    session.close();

  }
}
