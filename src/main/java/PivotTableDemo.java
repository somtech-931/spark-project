import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PivotTableDemo {

  public static void main(String[] args) {

    /*1. Create Spark session*/
    SparkSession session = SparkSession.builder().appName("DataFrameAPIDemo").master("local[*]").getOrCreate();

    /*2. Read big data log file*/
    Dataset<Row> bigLogDataRaw =
        session.read().option("header", true).csv("/home/hvadmin/My_Learning/Spark-Learning/biglog.txt");
    //bigLogDataRaw.show();

    Dataset<Row> formattedDS = bigLogDataRaw.select(col("level"), date_format(col("datetime"), "MMMM").alias("MONTH"));
    // formattedDS.show();

    Dataset<Row> pivotDS = formattedDS.groupBy(col("level")).pivot("MONTH").count();
    pivotDS.show();

    List<Object> monthNames = new ArrayList<>();
    monthNames.add("January");
    monthNames.add("February");
    monthNames.add("March");
    monthNames.add("April");
    monthNames.add("May");
    monthNames.add("June");
    monthNames.add("July");
    monthNames.add("August");
    monthNames.add("September");
    monthNames.add("October");
    monthNames.add("November");
    monthNames.add("December");

    System.out.println("");

    Dataset<Row> pivotDSOrdered = formattedDS.groupBy(col("level")).pivot("MONTH", monthNames).count();
    pivotDSOrdered.show();

    session.close();
  }
}
