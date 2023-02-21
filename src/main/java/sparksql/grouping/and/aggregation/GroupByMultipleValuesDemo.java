package sparksql.grouping.and.aggregation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GroupByMultipleValuesDemo {

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

    System.out.println("");

    /*3. Add extra column for aggregation*/
    Dataset<Row> dataset = session.sql("select level, date_format(datetime,'MMMM') AS MONTH, 1 as occurrence from logs_view_table");
    dataset.show();

    System.out.println("");

    /*4.Aggregate the COUNT part
    * Now point to note is we will group by level and month.
    * But month does not exist directly in the Dataset. We were creating it as an alias.
    * Therefore from the Dataset on the previous stage we need to create another view.*/
      dataset.createOrReplaceTempView("logs_view_table");

    /*5. Then on this view we can apply the group by on month*/
    Dataset<Row> groupedData =
        session.sql("select level, month, count(occurrence) as TOTALS from logs_view_table group by level, month");
    groupedData.show();

    System.out.println("");

    /*6.Check correctness
    * We have 1 million records. So to check correctness
    * we can add up the totals and see if it adds upto 1 million.*/
    groupedData.createOrReplaceTempView("log_table_validity");
    Dataset<Row> totalCount = session.sql("select sum(TOTALS) as TOTALCOUNT from log_table_validity");
    totalCount.show();


    session.close();

  }
}
