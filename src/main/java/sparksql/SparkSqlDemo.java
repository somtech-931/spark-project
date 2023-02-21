package sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlDemo {

  public static void main(String[] args) {

    /*Initializing Spark Context for SparkSQL*/
    SparkSession spark = SparkSession.builder().appName("sparkSqlDemo").master("local[*]").getOrCreate();

    /*Point Spark to the datasource, which is students.csv here using read() method
    * The return type is Dataset, which is very much like an abstraction of the data
    * This Dataset will have rows of data*/
    Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

    System.out.println("");

    /*Printing count of the number of rows in dataset*/
    System.out.println("Number of records in dataset: " + dataset.count());

    /*Show will show 1st 20 rows from the many 1000s of rows*/
    dataset.show();

    /*Get first row of the dataset*/
    Row firstRow = dataset.first();

    /*Get one column of that 1st row*/
    String subject = firstRow.get(2).toString();
    System.out.println("subject:"+subject);

    System.out.println("");

    /*Getting a column by column name: use getAs() method*/
    String subject1 = firstRow.getAs("subject").toString();
    System.out.println("subject1:"+subject1);

    System.out.println("");

    int year = Integer.parseInt(firstRow.getAs("year").toString());
    System.out.println("year:"+year);



    spark.close();
  }
}
