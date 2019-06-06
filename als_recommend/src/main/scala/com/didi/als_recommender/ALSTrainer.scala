package com.didi.als_recommender


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.jblas.DoubleMatrix

/**
  * 计算歌曲相似度矩阵
  *
  */

case class Recommendation(product: Int, rating: Double)

case class UserRecommendation(uid: Int, recommendations: Seq[Recommendation])

case class SongRecommendation(mid: Int, recommendations: Seq[Recommendation])


object ALSTrainer {


  def main(args: Array[String]): Unit = {

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

      val spark = SparkSession.builder()
        .master("local[6]")
        .enableHiveSupport()
        .getOrCreate()

      import spark.implicits._

      val ratings = spark.sql("select * from music_recommend_app.music_play_model limit 1000")

      val users = ratings
        .select($"user_id")
        .distinct
        .map(r => r.getAs[Int]("user_id"))
        .cache

      val songs = ratings
        .select($"track_id")
        .distinct
        .map(r => r.getAs[Int]("track_id"))
        .cache

      val trainData = ratings.map{ line =>
        Rating(line.getAs[Int]("user_id"), line.getAs[Int]("track_id"), line.getAs[Double]("score"))
      }.rdd.cache()

      val (rank, iterations, lambda) = (50, 5, 0.01)
      val model = ALS.train(trainData, rank, iterations, lambda)
      val maxRecs = 10



      val res1: DataFrame = calculateUserRecs(maxRecs, model, users, songs)
      val res2: DataFrame = calculateProductRecs(maxRecs, model, songs)
      res1.printSchema()
      res1.show(10)
      res2.printSchema()
      res2.show(10)




      spark.close()

  }
  /**
    * 余弦相似度，用于求歌曲相似度
    * @param vec1
    * @param vec2
    * @return
    */
  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }

  /**
    * 计算歌曲相似度矩阵
    * @param maxRecs  最大推荐数
    * @param model
    * @param products
    */
  private def calculateProductRecs(maxRecs: Int, model: MatrixFactorizationModel, products: Dataset[Int]): DataFrame = {

    import products.sparkSession.implicits._

    object RatingOrder extends Ordering[(Int, Int, Double)] {
      def compare(x: (Int, Int, Double), y: (Int, Int, Double)) = y._3 compare x._3
    }

    val productsVectorRdd = model.productFeatures
      .map{case (movieId, factor) =>
        val factorVector = new DoubleMatrix(factor)
        (movieId, factorVector)
      }

    val minSimilarity = 0.6

    val songRecommendation = productsVectorRdd.cartesian(productsVectorRdd)
      .filter{ case ((songId1, vector1), (songId2, vector2)) => songId1 != songId2 }
      .map{case ((songId1, vector1), (songId2, vector2)) =>
        val sim = cosineSimilarity(vector1, vector2)
        (songId1, songId2, sim)
      }.filter(_._3 >= minSimilarity)
      .groupBy(p => p._1)
      .map{ case (mid:Int, predictions:Iterable[(Int,Int,Double)]) =>
        val recommendations = predictions.toSeq.sorted(RatingOrder)
          .take(maxRecs)
          .map(p => Recommendation(p._2, p._3.toDouble))
        SongRecommendation(mid, recommendations)
      }.toDF()

    songRecommendation

  }


  val MAX_RECOMMENDATIONS: Int = 10

  /**
    * 计算为用户推荐的音乐集合矩阵 RDD[UserRecommendation(id: Int, recs: Seq[Rating])]
    *
    * @param maxRecs
    * @param model
    * @param users
    * @param products
    * @return
    */
  private def calculateUserRecs(maxRecs: Int, model: MatrixFactorizationModel, users: Dataset[Int], products: Dataset[Int]): DataFrame = {

    import users.sparkSession.implicits._

    val userProductsJoin = users.crossJoin(products)

    val userRating = userProductsJoin.map { row => (row.getAs[Int](0), row.getAs[Int](1)) }.rdd

    object RatingOrder extends Ordering[Rating] {
      def compare(x: Rating, y: Rating) = y.rating compare x.rating
    }

    val recommendations = model.predict(userRating)
      .filter(_.rating > 0)
      .groupBy(p => p.user)
      .map{ case (uid, predictions) =>
        val recommendations = predictions.toSeq.sorted(RatingOrder)
          .take(MAX_RECOMMENDATIONS)
          .map(p => Recommendation(p.product, p.rating))

        UserRecommendation(uid, recommendations)
      }.toDF()
    recommendations
  }

}


