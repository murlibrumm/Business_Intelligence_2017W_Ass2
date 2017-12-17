import java.io.File
import java.io.PrintWriter

import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object MovieRecommendations{
  def main(args: Array[String]): Unit = {

    import spark.implicits._


    var ratings_df = spark.read.text("../datasets/ml-latest/ratings.csv").
      select(split($"value", ",").as("value")).
      select(
        $"value".getItem(0).cast(IntegerType).as("user"),
        $"value".getItem(1).cast(IntegerType).as("product"),
        $"value".getItem(2).cast(FloatType).as("rating"))
    var header = ratings_df.first()
    ratings_df = ratings_df.filter(_ (0) != header(0))

    var movies_old = "../datasets/ml-latest/movies.csv"
    val movies_tmp = new File("../datasets/ml-latest/movies_tmp.csv")
    var writer = new PrintWriter(movies_tmp)
    Source.fromFile(movies_old)("UTF-8").getLines
      .map {
        x => x.replaceAll(", ", ";;").replaceAll(",", "::").replaceAll(";;", ", ")
      }.foreach(x => writer.println(x))
    writer.close()

    var movies_df = spark.read.text("../datasets/ml-latest/movies_tmp.csv").
      select(split($"value", "::").as("value")).
      select(
        $"value".getItem(0).cast(IntegerType).as("movie"),
        $"value".getItem(1).as("title"),
        split($"value".getItem(2), "\\|").as("genre"))
    header = movies_df.first()
    movies_df = movies_df.filter(_ (0) != header(0))


    var ratings_small_df = spark.read.text("../datasets/ml-latest-small/ratings.csv").
      select(split($"value", ",").as("value")).
      select(
        $"value".getItem(0).cast(IntegerType).as("user"),
        $"value".getItem(1).cast(IntegerType).as("product"),
        $"value".getItem(2).cast(FloatType).as("rating"))
    header = ratings_small_df.first()
    ratings_small_df = ratings_small_df.filter(_ (0) != header(0))

    movies_old = "../datasets/ml-latest-small/movies.csv"
    val movies_small_tmp = new File("../datasets/ml-latest-small/movies_tmp.csv")
    writer = new PrintWriter(movies_small_tmp)
    Source.fromFile(movies_old)("UTF-8").getLines
      .map {
        x => x.replaceAll(", ", ";;").replaceAll(",", "::").replaceAll(";;", ", ")
      }.foreach(x => writer.println(x))
    writer.close()

    var movies_small_df = spark.read.text("../datasets/ml-latest-small/movies_tmp.csv").
      select(split($"value", "::").as("value")).
      select(
        $"value".getItem(0).cast(IntegerType).as("movie"),
        $"value".getItem(1).as("title"),
        split($"value".getItem(2), "\\|").as("genre"))
    header = movies_small_df.first()
    movies_small_df = movies_small_df.filter(_ (0) != header(0))

    val ratings = ratings_df
    val movies = movies_df

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

	  val ratingsMax = Seq(
			("Prestige, The (2006)", 5.0),
			("Thing, The (1982)", 5.0),
			("Memento (2000)", 5.0),
			("From Dusk Till Dawn (1996)", 5.0),
			("Star Wars: Episode II - Attack of the Clones (2002)", 2.0),
			("Shawshank Redemption, The (1994)", 5.0),
			("Shining, The (1980)", 3.0),
			("The Similars (2015)", 3.0),
			("Devil (2010)", 2.0),
			("[REC] (2007)", 5.0),
			("[REC]² (2009)", 4.0),
			("Quarantine 2: Terminal (2011)", 1.0),
			("Book of Eli, The (2010)", 4.0),
			("Sucker Punch (2011)", 1.0),
			("Hot Fuzz (2007)", 5.0),
			("Ender's Game (2013)", 4.0),
			("Ghostbusters (a.k.a. Ghost Busters) (1984)", 5.0),
			("The Dark Knight (2011)", 5.0),
			("1408 (2007)", 4.0),
			("Godzilla (2014)", 1.0),
			("Station, The (Blutgletscher) (2013)", 4.0),
			("Cheap Thrills (2013)", 4.0),
			("2 Fast 2 Furious (Fast and the Furious 2, The) (2003)", 2.0),
			("Scary Movie 5 (Scary MoVie) (2013)", 2.0),
			("Pacific Rim (2013)", 2.0)
    ).toDF("title", "rating")

		val ratingsWolfi = Seq(
			("Harry Potter and the Deathly Hallows: Part 2 (2011)", 5.0),
			("Harry Potter and the Deathly Hallows: Part 1 (2010)", 5.0),
			("Lion King, The (1994)", 5.0),
			("Toy Story (1995)", 5.0),
			("Cars (2006)", 5.0),
			("Robin Hood (1972)", 5.0),
			("Monsters, Inc. (2001)", 5.0),
			("Bambi (1942)", 2.0),
			("Despicable Me (2010)", 5.0),
			("Rush (2013)", 4.0),
			("Senna (2010)", 4.0),
			("Star Wars: Episode III - Revenge of the Sith (2005)", 5.0),
			("Star Wars: Episode VII - The Force Awakens (2015)", 5.0),
			("Borat: Cultural Learnings of America for Make Benefit Glorious Nation of Kazakhstan (2006)", 5.0),
			("Anabelle (2014)", 2.0),
			("Inglorious Basterds (2009)", 5.0),
			("Django Unchained (2012)", 4.0),
			("Forrest Gump (1994)", 5.0),
			("Clockwork Orange (1971)", 3.0),
			("American Sniper (2014)", 2.0),
			("Suicide Squad (2016)", 3.0),
			("American Pie (1999)", 1.0),
			("Scary Movie (2000)", 2.0),
			("Avengers, The (2012)", 2.0),
			("Click (2006)", 1.0)
    ).toDF("title", "rating")

    val userId = 0
    val normalizedRatings = ratingsWolfi.
      join(movies, "title").
      select(lit(userId).as("user"), $"movie".as("product"), $"rating")
    val movieIds = normalizedRatings.select("product").collect.map(_.getAs[Int]("product")).toSeq


    val set = ratings.randomSplit(Array(0.9, 0.1), seed = 1L)
    val training = normalizedRatings.union(set(0)).cache()
    val test = set(1).cache()


    val trainingRDD = training.as[Rating].rdd
    var usersProducts = movies.select(lit(userId), $"movie").map { row => (row.getInt(0), row.getInt(1)) }.toJavaRDD

    val rank = 15
    val numIterations = 13
    val model = ALS.train(trainingRDD, rank, numIterations, 0.1)


    val result = model.predict(usersProducts).toDS()


    val df = result.filter { rating => rating.user == 0 }.toDF()

    df.toDF().sort($"rating".desc).join(movies, df("product") === movies("movie"), "inner").select("movie", "title", "genre").show(false)

    movies_tmp.delete()
    movies_small_tmp.delete()
  }
}
