package sparksql.dataset.filters;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LambdaFilterDemo {

  public static void main(String[] args) {

    /*Create SparksSession*/
    SparkSession session = SparkSession.builder().appName("Dataset-LambdaFilterDemo").master("local[*]").getOrCreate();

    /*Read input data file*/
    Dataset<Row> studentsDataset = session.read()
                                          .option("header", true)
                                          .csv("src/main/resources/exams/students.csv");

    /*Filter students dataset based on condition:
     * 1.subject = "Modern art"
     * 2.year > 2007
     * Like RDDs Datasets are also immutable, so after filtering we need to hold it in a separate Dataset.*/
    Dataset<Row> modernArtStuds = studentsDataset
        .filter((FilterFunction<Row>) row -> ("Modern Art").equalsIgnoreCase(row.getAs("subject").toString())
            && Integer.parseInt(row.getAs("year")) >= 2007);

    /*Show results of the filtered Dataset
     * This will show only the 1st 20 records.*/
    modernArtStuds.show();

    System.out.println("");

    /*Count of students in the raw data who had modern art in their curriculum.*/
    System.out.println("Number of students opting modern art in 2007: "+modernArtStuds.count());
  }
}
