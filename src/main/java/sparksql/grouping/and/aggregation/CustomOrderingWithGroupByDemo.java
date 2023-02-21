package sparksql.grouping.and.aggregation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CustomOrderingWithGroupByDemo {

  public static void main(String[] args) {

    /*1.Create Spark Session*/
    SparkSession session = SparkSession.builder().appName("CustomOrderingWithGroupByDemo").master("local[*]").getOrCreate();

    /*2.Load data from a file*/
    Dataset<Row> bigLogFileRaw =
        session.read().option("header", true).csv("/home/hvadmin/My_Learning/Spark-Learning/biglog.txt");

    /*3.Create the temporary view table*/
    bigLogFileRaw.createOrReplaceTempView("log_file_view");

    /*4.Grouped Data by level and MONTH*/
    Dataset<Row> groupedData = session.sql("select level,date_format(datetime,'MMMM') AS MONTH,count(1) as count from log_file_view group by level,MONTH");
    //groupedData.show();

    System.out.println("");

    /*5. Order by month (alphabetical)*/

    groupedData.createOrReplaceTempView("grouped_view");
    Dataset<Row> orderedByMonthAlph = session.sql("select level, month, count from grouped_view order by MONTH");
    //orderedByMonthAlph.show();


    System.out.println("");

    /*6. Order by month (numeric)*/
    Dataset<Row> groupedAndOrderedData = session.sql("select level, date_format(datetime,'MMMMM') AS MONTH, cast(first(date_format(datetime,'M')) as int) AS MONTHNUM, count(1) as count from log_file_view group by level,MONTH order by MONTHNUM");
    //groupedAndOrderedData.show(100);

    System.out.println("");

    /*7. Order by month (numeric) and level*/
    Dataset<Row> groupedAndOrderedDataByLevelAndMonth = session.sql("select level, date_format(datetime,'MMMMM') AS MONTH, cast(first(date_format(datetime,'M')) as int) AS MONTHNUM, count(1) as count from log_file_view group by level,MONTH order by MONTHNUM, level");
    //groupedAndOrderedDataByLevelAndMonth.show(100);

    System.out.println("");

    /*Hiding columns*/
    /*1. Drop column*/
    Dataset<Row> monthNumDroppedDS = groupedAndOrderedDataByLevelAndMonth.drop("MONTHNUM");
    monthNumDroppedDS.show(100);

    System.out.println("");

    /*2. Using function in order by clause*/
    groupedAndOrderedDataByLevelAndMonth.createOrReplaceTempView("final_view");
    Dataset<Row> finalDS = session.sql(
        "select level, date_format(datetime,'MMMMM') AS MONTH, count(1) as count from log_file_view group by level,MONTH order by cast(first(date_format(datetime,'M')) as int) , level");
    finalDS.show(100);

    session.close();
  }
}