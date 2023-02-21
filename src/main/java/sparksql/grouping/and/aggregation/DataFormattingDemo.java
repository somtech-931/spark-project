package sparksql.grouping.and.aggregation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFormattingDemo {

  public static void main(String[] args) {

    /*1.Create Spark Session*/
    SparkSession session = SparkSession.builder().appName("DataFormattingDemo").master("local[*]").getOrCreate();

    /*2.Load data from a file*/
    Dataset<Row> logFileRawData =
        session.read().option("header", true).csv("/home/hvadmin/My_Learning/Spark-Learning/biglog.txt");

    logFileRawData.show();

    /*Data Formatting*/
    /*Assignment: We want to group the log levels based on the month names*/

    /*1.Create the temporary view table*/
    logFileRawData.createOrReplaceTempView("logs_view_table");

    System.out.println("");

    /*2. Apply sql and write formatting*/
    Dataset<Row> dateFormattedToMonth = session.sql("select level, date_format(datetime,'MMMM') AS MONTH from logs_view_table");
    dateFormattedToMonth.show();

    session.close();
  }
}
