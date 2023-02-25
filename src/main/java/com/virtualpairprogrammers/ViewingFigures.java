package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = true;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		// TODO - over to you!
		viewData = viewData.distinct();
		//viewData.collect().forEach(System.out::println);
		System.out.println();
		//chapterData.collect().forEach(System.out::println);
		System.out.println();

		JavaPairRDD<Integer, Integer> flippedViewRdd =
				viewData.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> join = flippedViewRdd.join(chapterData);
		//join.collect().forEach(System.out::println);
		System.out.println();

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> reducedRDD =
				join.mapToPair(tuple -> new Tuple2<>(tuple._2, 1)).reduceByKey((val1, val2) -> val1 + val2);
		//reducedRDD.collect().forEach(System.out::println);

		System.out.println();

		JavaPairRDD<Integer, Integer> chaptersPerCourseRDD =
				chapterData.mapToPair(tuple -> new Tuple2<>(tuple._2, 1))
						.reduceByKey((val1, val2) -> (val1 + val2));
		chaptersPerCourseRDD.collect().forEach(System.out::println);

		System.out.println();

		JavaPairRDD<Integer, Integer> courseViewsRDD =
				reducedRDD.mapToPair(tuple -> new Tuple2<>(tuple._1._2, tuple._2))
						  .reduceByKey((val1,val2) -> val1+val2);
		//courseViewsRDD.collect().forEach(System.out::println);

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joined = chaptersPerCourseRDD.join(courseViewsRDD);
		joined.collect().forEach(System.out::println);
		System.out.println();

		JavaRDD<Tuple2<Integer, Integer>> results =
				joined.map(row -> new Tuple2<>(row._1, calculateScore(row._2._1, row._2._2)));
		results.collect().forEach(System.out::println);
		sc.close();
	}

	private static int calculateScore(int chapterCount, int views){

		int viewPercentage = views * 100 / chapterCount;
		if(viewPercentage<25) return 0;
		if(viewPercentage>25 && viewPercentage<50) return 2;
		if(viewPercentage>50 && viewPercentage<90) return 4;
		if(viewPercentage>95) return 10;
		return 0;
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
