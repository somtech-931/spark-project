package sparksql.grouping.and.aggregation;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class GroupAggregationBasicDemo {

  public static void main(String[] args) {

    /*Create Spark Session*/
    SparkSession session = SparkSession.builder().appName("GroupAggregationDemo").master("local[*]").getOrCreate();

    /*Create In Memory data*/

    /*1. RowFactory.create method to create the individual rows*/
    Row row1 = RowFactory.create("WARN", "2016-12-31 04:19:32");
    Row row2 = RowFactory.create("FATAL", "2016-12-31 03:22:34");
    Row row3 = RowFactory.create("WARN", "2016-12-31 03:21:21");
    Row row4 = RowFactory.create("INFO", "2015-4-21 14:32:21");
    Row row5 = RowFactory.create("FATAL","2015-4-21 19:23:20");

    /*2. Create a List to hold these data*/
    List<Row> inMemoryData = new ArrayList<>();
    inMemoryData.add(row1);
    inMemoryData.add(row2);
    inMemoryData.add(row3);
    inMemoryData.add(row4);
    inMemoryData.add(row5);

    /*3.Create StructField for each row data
    * Since we have 2 String fields in the rows, we will create 2 StructFields*/
    StructField sf1 = new StructField("level", DataTypes.StringType,false, Metadata.empty());
    StructField sf2 = new StructField("date_time", DataTypes.StringType,false, Metadata.empty());

    /*4.Create fields array*/
    StructField[] fields = {sf1,sf2};

    /*5. Create a schema with these fields*/
    StructType schema = new StructType(fields);

    /*6.Create a DataFrame out of the above fields and StructType*/
    Dataset<Row> dataFrame = session.createDataFrame(inMemoryData, schema);
    dataFrame.show();

    /*7. Create a temp view to perform sql operations/commands*/
    dataFrame.createOrReplaceTempView("logs_temp_view");

    System.out.println();

    /*8. Execute sql statements*/
    Dataset<Row> allRecords = session.sql("select level, date_time from logs_temp_view");
    allRecords.show();

    System.out.println();

    /*9. Now do aggregation and group by on the log_temp_view.
    * We want to count number of logs per level*/
    Dataset<Row> groupedData = session.sql("select level, count(date_time) as count from logs_temp_view group by level");
    groupedData.show();

    System.out.println();

    /*10. We can also order the data based on log level alphabetical order*/
    Dataset<Row> groupedAndOrderedData =
        session.sql("select level, count(*) as count from logs_temp_view group by level order by level asc");
    groupedAndOrderedData.show();

    session.close();
  }
}
