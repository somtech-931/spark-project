package userDefinedFunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class UserDefinedFunctionsDemo {

  public static void main(String[] args) {

    /*1. Build Spark Session*/
    SparkSession session = SparkSession.builder().appName("UserDefinedFunctionsDemo").master("local[*]").getOrCreate();

    /*2. Read students big data file*/
    Dataset<Row> studentsRawData = session.read().option("header", true).csv("src/main/resources/exams/students.csv");

    /*Dataset<Row> newRowAddedDS = studentsRawData.withColumn("Pass", functions.lit("Yes"));
    newRowAddedDS.show(5);*/

    /*Dataset<Row> newRowAddedDS = studentsRawData.withColumn("Pass", lit(col("grade").equalTo("A+")));
    newRowAddedDS.show(15);*/

    /*session.udf().register("hasPassed", (String grade) -> grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C") ,
        DataTypes.BooleanType);
    Dataset<Row> passDataDS = studentsRawData.withColumn("PASS", callUDF("hasPassed", col("grade")));
    passDataDS.show(10);*/

    session.udf().register("hasPassed", (String subject,String grade) -> {
      return ("Biology".equalsIgnoreCase(subject) ? (grade.startsWith("A") || grade.startsWith("B"))
          : grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C"));
    }, DataTypes.BooleanType);

    Dataset<Row> passDataDS = studentsRawData.withColumn("PASS", callUDF("hasPassed", col("subject"), col("grade")));
    passDataDS.show(20);

    session.close();
  }
}
