package sparksql.temporary.view;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TemporaryViewSqlDemo {

  public static void main(String[] args) throws AnalysisException {

    /*Create SparksSession*/
    SparkSession session = SparkSession.builder().appName("TemporaryViewSqlDemo").master("local[*]").getOrCreate();

    /*Read input data file*/
    Dataset<Row> studentData = session.read().option("header", true).csv("src/main/resources/exams/students.csv");

    /*Here at this step we are creating a temporary view from the Dataset
    * students_view is the name of the view.
    * createTempView() is a void method.*/
    studentData.createTempView("students_view");

    /*Now on this view we can do full sql operations*/
    /*1. Get all students data*/
    Dataset<Row> allStudentsData = session.sql("select * from students_view");
    allStudentsData.show();

    System.out.println();

    /*2. Sql statement with where clause: Get all students with French subject*/
    Dataset<Row> frenchStudents = session.sql("select * from students_view where subject = 'French'");
    frenchStudents.show();
    System.out.println();

    /*3.Get highest score in French over the years*/
    Dataset<Row> highestFrenchScore = session.sql("select max(score) from students_view where subject = 'French'");
    highestFrenchScore.show();
    System.out.println("");

    /*4.Get min scores in Modern Art*/
    Dataset<Row> modernArtMinScore = session.sql("select min(score) as MIN_MODER_ART_SCORE from students_view where subject = 'Modern Art'");
    modernArtMinScore.show();
    System.out.println("");

    /*4.Get max scores in Modern Art*/
    Dataset<Row> modernArtMaxScore = session.sql("select max(score) as MAX_MODER_ART_SCORE from students_view where subject = 'Modern Art'");
    modernArtMaxScore.show();
    System.out.println("");

    /*Get avg score in Biology*/
    Dataset<Row> biologyAvgScore = session.sql("SELECT avg(score) AS AVERAGE_BIOLOGY_SCORE from students_view where subject = 'Biology'");
    biologyAvgScore.show();
    System.out.println("");

    /*Get years of score collecting in ascending order*/
    Dataset<Row> year_asc = session.sql("SELECT distinct(year) as YEAR_IN_ASC_ORDER FROM students_view order by YEAR_IN_ASC_ORDER asc");
    year_asc.show();
    System.out.println("");

    Dataset<Row> year_desc = session.sql("SELECT distinct(year) as YEAR_IN_DESC_ORDER FROM students_view order by YEAR_IN_DESC_ORDER desc");
    year_desc.show();
  }

}
