package sparksql;

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

public class InMemoryDataCreationDemo {

  public static void main(String[] args) {

    /*Create SparksSession*/
    SparkSession session = SparkSession.builder().appName("TemporaryViewSqlDemo").master("local[*]").getOrCreate();

    /*List to hold inMemory Data*/
    List<Row> inMemoryData = new ArrayList<>();

    /*RowFactory.create method to create the individual rows*/
    Row row1 = RowFactory.create("WARN", "2016-12-31 04:19:32");
    Row row2 = RowFactory.create("FATAL", "2016-12-31 03:22:34");
    Row row3 = RowFactory.create("WARN", "2016-12-31 03:21:21");
    Row row4 = RowFactory.create("INFO", "2015-4-21 14:32:21");
    Row row5 = RowFactory.create("FATAL","2015-4-21 19:23:20");

    /*Add these rows into the list*/
    inMemoryData.add(row1);
    inMemoryData.add(row2);
    inMemoryData.add(row3);
    inMemoryData.add(row4);
    inMemoryData.add(row5);

    /*Now we need to tell Spark about the schema of the input data*/
    /*1. We define the StructFields.
    * There will be as many number of StructFields as the number of columns.
    * Since the logging data has 2 fields: we define 2 StructFields*/
    StructField sf1 = new StructField("level", DataTypes.StringType,false, Metadata.empty());
    StructField sf2 = new StructField("date-time", DataTypes.StringType,false, Metadata.empty());
    StructField[] structFields = {sf1,sf2};

    /*2. Here we define the schema with the Struct Fields*/
    StructType schema = new StructType(structFields);

    /*Finally we create the dataframe/dataset with the data and its schema defined*/
    Dataset<Row> dataSet = session.createDataFrame(inMemoryData, schema);

    dataSet.show();
  }
}
