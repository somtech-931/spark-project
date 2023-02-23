package dataframeAPI;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class DataFrameAPIDemo {

  public static void main(String[] args) {

    /*1. Create Spark session*/
    SparkSession session = SparkSession.builder().appName("DataFrameAPIDemo").master("local[*]").getOrCreate();

    /*2. Read big data log file*/
    Dataset<Row> bigLogDataRaw =
        session.read().option("header", true).csv("/home/hvadmin/My_Learning/Spark-Learning/biglog.txt");
    //bigLogDataRaw.show();

    /*System.out.println("");
    3. Using select function*/
    /*Dataset<Row> levelsColumn = bigLogDataRaw.select("level");
    levelsColumn.show();*/

    /*4. Format data time*/
    /*Dataset<Row> formattedDate = bigLogDataRaw.select(col("level"), date_format(col("datetime"), "MMMM"));
    formattedDate.show();*/

    /*5. Group data by monthnum and level*/
    /*Dataset<Row> groupedData =
        bigLogDataRaw.select(col("level"),
            date_format(col("datetime"), "MMMM").alias("MONTH"))
            .groupBy(col("MONTH"), col("level"))
            .count();
    groupedData.show(100);*/

    /*6. Order data by month num*/
    /*Dataset<Row> groupedData = bigLogDataRaw.select(col("level"),
        date_format(col("datetime"), "MMMM").alias("MONTH"),
        date_format(col("datetime"), "M").alias("MONTHNUM").cast(DataTypes.IntegerType))
        .groupBy(col("MONTH"), col("level"),col("MONTHNUM"))
        .count();

    groupedData.show(200);*/

    /*System.out.println("");

    Dataset<Row> orderedData = bigLogDataRaw.select(col("level"),
        date_format(col("datetime"), "MMMM").alias("MONTH"),
        date_format(col("datetime"), "M").alias("MONTHNUM").cast(DataTypes.IntegerType))
        .groupBy(col("MONTH"), col("level"),col("MONTHNUM"))
        .count().orderBy("MONTHNUM","level");
    orderedData.show(200);*/

    /*7. Drop numeric month from result*/
    Dataset<Row> finalData = bigLogDataRaw.select(col("level"),
        date_format(col("datetime"), "MMMM").alias("MONTH"),
        date_format(col("datetime"), "M").alias("MONTHNUM").cast(DataTypes.IntegerType))
        .groupBy(col("MONTH"), col("level"),col("MONTHNUM"))
        .count().orderBy("MONTHNUM","level").drop("MONTHNUM");
    finalData.show(200);
    session.close();
  }
}
