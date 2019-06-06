package com.didi.als_recommender

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


/**
  * 推荐程序
  * 通过传入参数的方式分别得到不同的推荐结果
  */
object Recommender {

  /**
    * 程序的入口
    * @param args
    *  此程序需传入两个参数
    *  arg[0]:模型数据表  db.table 格式
    *  arg[1]:存储目标表  db.table 格式
    */
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    //1.创建spark连接
    val spark = SparkSession.builder()
      .appName("Recommender")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //2.读取数据
    val dataframe: DataFrame = read_hive(spark,args(0))

    import spark.implicits._

    //3.转换数据集
    val ALL_SIMPLE: RDD[Rating] = caver2Rating(dataframe)
    //4.拆分数据集为训练集，验证集和测试集
    val Array(training, validation, test) = ALL_SIMPLE.randomSplit(Array(0.6, 0.2 ,0.2))
    training.cache()
    validation.cache()
    test.cache()
    //5.输出数据的基本信息（所有）
    val NUM_ALL_SIMPLE = ALL_SIMPLE.count()
    val NUM_ALL_USER = ALL_SIMPLE.map(_.user).distinct().count()
    val NUM_ALL_ITEM = ALL_SIMPLE.map(_.product).distinct().count()

    println("总样本基本信息为：")
    println("总样本数："+NUM_ALL_SIMPLE)
    println("总用户数："+NUM_ALL_USER)
    println("总推荐对象数："+NUM_ALL_ITEM)

    val NUM_TRAIN_SIMPLE = training.count()
    val NUM_VALIDA_SIMPLE = validation.count()
    val NUM_TEST_SIMPLE = test.count()

    println("验证样本基本信息为：")
    println("训练样本数："+NUM_TRAIN_SIMPLE)
    println("验证样本数："+NUM_VALIDA_SIMPLE)
    println("测试样本数："+NUM_TEST_SIMPLE)

    //5.创建一个训练模型，并从指定的参数集中找到最优参数，即RMSE最小的时候的参数
    val ranks = List(5,10,15,20)
    val lambdas = List(0.01,0.1)
    val numiters = List(5,10)
    var bestModel:Option[MatrixFactorizationModel]=None
    var bestValidationRmse=Double.MaxValue
    var bestRank=0
    var bestLamdba = -1.0
    var bestNumIter=1

    //6.用验证数据集验证训练模型的参数
    for(rank<-ranks;lambda<-lambdas;numiter<-numiters) {
      println(rank + "-->" + lambda + "-->" + numiter)
      val model: MatrixFactorizationModel = ALS.train(training, rank, numiter, lambda)
      val valadationRmse = computeRmse(model, validation)
      if (valadationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = valadationRmse
        bestRank = rank
        bestLamdba = lambda
        bestNumIter = numiter
      }
    }

    //7.测试数据集测试模型的误差大小并得到最优参数
    val testRmse = computeRmse(bestModel.get,test)
    println("测试数据的rmse为："+testRmse)
    println("范围内的最后模型参数为：")
    println("隐藏因子数："+bestRank)
    println("正则化参数："+bestLamdba)
    println("迭代次数："+bestNumIter)

    //8.计算训练样本和验证样本的平均分数
    val meanR = training.union(validation).map(x=>x.rating).mean()

    //这就是使用平均分做预测，test样本的rmse
    val baseRmse=math.sqrt(test.map(x=>(meanR-x.rating)*(meanR-x.rating)).mean())

    //val improvement =(baseRmse-testRmse)/baseRmse*100

   // println("使用了ALS协同过滤算法比使用评价分作为预测的提升度为："+improvement)

    //9.使用最优参数进行推荐
    val resModel: MatrixFactorizationModel = ALS.train(ALL_SIMPLE,bestRank,bestNumIter,bestLamdba)


    //针对不同的模型推荐不同数量的结果
    var num = 0
    if(args(0) == "music_recommend_app.genre_col_model"){num = 5}
    if(args(0) == "music_recommend_app.music_num_model"){num = 50}
    if(args(0) == "music_recommend_app.music_play_model"){num = 50}
    if(args(0) == "music_recommend_app.rec_score_model"){num = 20}
    if(args(0) == "music_recommend_app.rec_cli_model"){num = 20}

    val resultRDD: RDD[(Int, Array[Rating])] = resModel.recommendProductsForUsers(num)


    //得到列名
    val col1 = get_schema(args(0),spark)(0)
    val col2 = get_schema(args(0),spark)(1)
    val col3 = "predict"
    val res: DataFrame = resultRDD.flatMapValues(x => x).values.toDF(col1,col2,col3)

    //10.新增自增id列
    res.createTempView("a")
    val result: DataFrame = spark.sql("select row_number() over(order by 1) as id,a.* from a")
    spark.sql("")
    result.printSchema()
    //11.将推荐结果保存到hive表
    insert_hive(spark,args(1),result)
    println("推荐结果已保存到hive")
    //12.将推荐结果保存到mysql表

    insert_mysql(spark,result,"rec_genre")
    println("推荐结果已保存到mysql")



    //关闭spark，程序结束
    spark.stop()

  }

  /**
    * 得到表头的方法
    * @param table
    * @param spark
    * @return
    */
  def get_schema(table:String,spark: SparkSession):Array[String] ={
    spark.table(table).columns
  }


  /**
    * 将推荐结果写入hive的方法
    * @param spark        sparksession对象
    * @param tableName    存到hive的目标表名
    * @param dataDF       结果集
    */
  def insert_hive(spark: SparkSession, tableName: String, dataDF: DataFrame): Unit = {
    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  /**
    * 读取hive数据的方法
    * @param spark        sparksession对象
    * @param tableName    读取的hive表名
    * @return      返回dataframe
    */
  def read_hive(spark: SparkSession,tableName: String):DataFrame ={
    spark.sql("SELECT * FROM " + tableName)
  }


  /**
    * 将读取的数据转换成Rating类
    * @param df  数据集
    * @return
    */
  def caver2Rating(df: DataFrame): RDD[Rating] = {
    df.rdd.map(_ match {
      case Row(userid,itemid,rate) =>
        Rating(userid.toString.toInt,itemid.toString.toInt,rate.toString.toDouble)
    })
  }

  /**
    * 定义RMSE方法
    * @param model  als模型
    * @param data   验证数据集
    * @return
    */
  def computeRmse(model:MatrixFactorizationModel,data:RDD[Rating]):Double= {
    val predictions: RDD[recommendation.Rating] = model.predict(data.map(x => (x.user,x.product)))
    val predictionRDD: RDD[((Int, Int), Double)] = predictions.map(x => ((x.user,x.product),x.rating))
    val dataRDD: RDD[((Int, Int), Double)] = data.map(x => ((x.user,x.product),x.rating))
    val predictionAndRatings : RDD[(Double, Double)] = predictionRDD.join(dataRDD).values
    math.sqrt(predictionAndRatings.map(x => (x._1 - x._2)*(x._1 - x._2)).mean())
  }

  /**
    * 将推荐结果保存到mysql
    * @param table    保存到的目标表名
    * @param df       结果集
    */
  def insert_mysql(spark:SparkSession,df:DataFrame,table:String) = {
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","1234")
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://10.10.30.100:3306/test",table,prop)

  }

/*  /**
    * 将推荐结果保存到mysql
    * @param dataFrame
    */
  def save2mysql(spark: SparkSession,dataFrame: DataFrame): Unit ={
    val properties = spark.read.jdbc("jdbc:mysql://119.23.151.103:3306/ai","rec_gener",properties).sqlContext.sql("truncate table ")
    dataFrame.write.format("jdbc")
      .option("url","jdbc:mysql://119.23.151.103:3306/ai?characterEncoding=UTF-8")
      .option("dbtable","rec_gener")
      .option("user","didiphp")
      .option("password","DotDeeMy365com")
      .mode(SaveMode.Append)
      .save()
  }*/


}
