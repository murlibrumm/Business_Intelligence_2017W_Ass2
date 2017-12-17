import java.io.File
import java.io.PrintWriter

import scala.io.Source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object MovieRecommendations{
	def main(args: Array[String]): Unit = {
	  import spark.implicits._


	  var ratings_df = spark.read.text("..\\datasets\\ml-latest\\ratings.csv").
		select(split($"value", ",").as("value")).
		select(
		  $"value".getItem(0).cast(IntegerType).as("user"),
		  $"value".getItem(1).cast(IntegerType).as("product"),
		  $"value".getItem(2).cast(FloatType).as("rating"))
	  var header = ratings_df.first()
	  ratings_df = ratings_df.filter(_(0) != header(0))
	  
	  var movies_df = spark.read.text("..\\datasets\\ml-latest\\movies.csv").
		select(split($"value", ",").as("value")).
		select(
		  $"value".getItem(0).cast(IntegerType).as("movie"),
		  $"value".getItem(1).as("title"),
		  split($"value".getItem(2), "\\|").as("genre"))
	  header = movies_df.first()
	  movies_df = movies_df.filter(_(0) != header(0))


	  var ratings_small_df = spark.read.text("..\\datasets\\ml-latest-small\\ratings.csv").
		select(split($"value", ",").as("value")).
		select(
		  $"value".getItem(0).cast(IntegerType).as("user"),
		  $"value".getItem(1).cast(IntegerType).as("product"),
		  $"value".getItem(2).cast(FloatType).as("rating"))
	  header = ratings_small_df.first()
	  ratings_small_df = ratings_small_df.filter(_(0) != header(0))

	  var movies_small_df = spark.read.text("..\\datasets\\ml-latest-small\\movies.csv").
		select(split($"value", ",").as("value")).
		select(
		$"value".getItem(0).cast(IntegerType).as("movie"),
		$"value".getItem(1).as("title"),
		split($"value".getItem(2), "\\|").as("genre"))
	  header = movies_small_df.first()
	  movies_small_df = movies_small_df.filter(_(0) != header(0))

	  ratings_df.show(false)
	  movies_df.show(false)
	  ratings_small_df.show(false)
	  movies_small_df.show(false)



	  val ratings = ratings_small_df
	  val movies = movies_small_df

	  val ratingsRoman = Seq(
		("Toy Story (1995)", 5.0),
		("Jumanji (1995)", 5.0),
		("Pocahontas (1995)", 3.0),
		("Aladdin (1992)", 4.0),
		("Harry Potter and the Deathly Hallows: Part 1 (2010)", 5.0),
		("Green Hornet, The (2011)", 5.0),
		("Winnie the Pooh and Tigger Too (1974)", 1.0),
		("Kung Fu Panda 2 (2011)", 5.0),
		("X-Men: First Class (2011)", 4.0),
		("Harry Potter and the Deathly Hallows: Part 2 (2011)", 5.0),
		("Smurfs, The (2011)", 2.0),
		("Avengers, The (2012)", 5.0),
		("Sixth Sense, The (1999)", 3.0),
		("Three Musketeers, The (2011)", 3.0),
		("John Carter (2012)", 5.0),
		("Godfather, The (1972)", 3.0),
		("Dictator, The (2012)", 5.0),
		("Silence of the Lambs, The (1991)", 3.0),
		("Brave (2012)", 5.0),
		("Planes (2013)", 2.0),
		("Zombeavers (2014)", 2.0),
		("Haunt (2013)", 1.0),
		("Interview with the Vampire: The Vampire Chronicles (1994)", 1.0),
		("Jurassic Park (1993)", 2.0),
		("Day of the Dead (1985)", 1.0)
	  ).toDF("title", "rating")
	  val userId = 0
	  val normalizedRatings = ratingsRoman.
		join(movies, "title").
		select(lit(userId).as("user"), $"movie".as("product"), $"rating")
	  val movieIds = normalizedRatings.select("product").collect.map(_.getAs[Int]("product")).toSeq


	  val set = ratings.randomSplit(Array(0.9, 0.1), seed = 1L)
	  val training = normalizedRatings.union(set(0)).cache()
	  val test = set(1).cache()

	  println(s"Training: ${training.count()}, test: ${test.count()}")


	  val trainingRDD = training.as[Rating].rdd
	  var usersProducts = movies.select(lit(userId), $"movie").map { row => (row.getInt(0), row.getInt(1)) }.toJavaRDD

	  val rank = 15
	  val numIterations = 13
	  val model = ALS.train(trainingRDD, rank, numIterations, 0.04)


	  val result = model.predict(usersProducts).toDS()
	  result.show()


	  val df = result.filter { rating => rating.user == 0 }.toDF()

	  df.toDF().sort($"rating".desc).join(movies, df("product") === movies("movie"), "inner").select("movie", "title", "genre").show(false)

	}
}
