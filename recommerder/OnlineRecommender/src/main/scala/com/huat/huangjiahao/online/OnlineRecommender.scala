package com.huat.huangjiahao.online

import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document
import redis.clients.jedis.Jedis

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`


// 定义一个连接助手对象，建立到redis和mongodb的连接
object ConnHelper extends Serializable {
  val host = "172.16.26.152"
  // 懒变量定义，使用的时候才初始化
  lazy val jedis = new Jedis(host)
  jedis.auth("12345678")
  lazy val mongoClient = MongoClients.create((s"mongodb://$host:27017/recommender"))
}

case class MongoConfig(uri: String, db: String)

// 定义标准推荐对象
case class Recommendation(productId: Int, score: Double)

// 定义用户的推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 定义商品相似度列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

/**
  * 实时推荐
  */
object OnlineRecommender {
  // 定义常量和表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val STREAM_RECS = "StreamRecs"
  val PRODUCT_RECS = "ProductRecs"

  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20

  private var simProcutsMatrixBC: Broadcast[scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]] = _

  private var mongoConfig: MongoConfig = _

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://tiger.local:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    // 创建spark conf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    OnlineRecommender.mongoConfig = mongoConfig

    // 加载数据，相似度矩阵，广播出去
    val simProductsMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("database", mongoConfig.db)
      .option("collection", PRODUCT_RECS)
      .format("mongodb")
      .load()
      .as[ProductRecs]
      .rdd
      // 为了后续查询相似度方便，把数据转换成map形式
      .map { item =>
      (item.productId, item.recs.map(x => (x.productId, x.score)).toMap)
    }
      .collectAsMap()
    // 定义广播变量
    OnlineRecommender.simProcutsMatrixBC = sc.broadcast(simProductsMatrix)

    // 创建kafka配置参数
    val kafkaParam = Map(
      "bootstrap.servers" -> "172.16.26.152:29092,172.16.26.152:39092,172.16.26.152:49092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaBrokers = kafkaParam("bootstrap.servers").toString
    val kafkaTopic = config("kafka.topic").toString

    import org.apache.spark.sql.functions._
    // 创建一个DStream
    val kafkaDF = spark.readStream
      // 使用内置的 Kafka 数据源
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic)
      .option("kafka.group.id", "recommender")
      .option("startingOffsets", "latest")
      .load()

    kafkaDF.printSchema()

    // 对kafkaStream进行处理，产生评分流，userId|productId|score|timestamp
    val ratingDF = kafkaDF
      // 提取 value 列，并转换为 String
      .selectExpr("CAST(value AS STRING) AS value_string", "timestamp")
      // 分割字符串，生成一个数组列
      .withColumn("attr", split(col("value_string"), "\\|"))
      .filter(size(col("attr")) >= 4)
      // 提取数组元素并转换为对应的类型，生成最终的结构化评分流
      .select(
        col("attr").getItem(0).cast("int").as("userId"),
        col("attr").getItem(1).cast("int").as("productId"),
        col("attr").getItem(2).cast("double").as("score"),
        col("attr").getItem(3).cast("int").as("timestamp")
      ).as[Rating]

    // 核心算法部分，定义评分流的处理流程
    /*ratingDF.as[(Int, Int, Double, Int)].rdd.foreach {
      case (userId, productId, score, timestamp) =>
        // 核心算法流程 1. 从redis里取出当前用户的最近评分
        val userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATING_NUM,
          userId, ConnHelper.jedis)
        // 2. 从相似度矩阵中获取当前商品最相似的商品列表，作为备选列表，保存成一个数组Array[productId]
        val candidateProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, productId, userId,
          simProcutsMatrixBC.value)
        // 3. 计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表，保存成 Array[(productId, score)]
        val streamRecs = computeProductScore(candidateProducts, userRecentlyRatings,
          simProcutsMatrixBC.value)
        // 4. 把推荐列表保存到mongodb
        saveDataToMongoDB(userId, streamRecs)

        // (其他原有逻辑，如 getTopSimProducts, computeProductScore, saveDataToMongoDB)
    }*/

    ratingDF.writeStream
      // 使用 foreachBatch 将每个微批次的 DataFrame 传递给自定义函数
      .foreachBatch(processRatingsBatch _)
      // 必须设置一个检查点路径，用于容错和记录偏移量
      .option("checkpointLocation", "./checkpoint/dir")
      // 定义触发间隔，例如每隔 2 秒检查一次新数据
      .trigger(Trigger.ProcessingTime(2, TimeUnit.SECONDS))
      // 启动流查询，返回一个 StreamingQuery 对象
      .start()
      .awaitTermination()

    // 启动streaming
    println("streaming started!")
  }

  /**
    * 从redis里获取最近num次评分
    */

  import scala.jdk.CollectionConverters._

  def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    // 从redis中用户的评分队列里获取评分数据，list键名为uid:USERID，值格式是 PRODUCTID:SCORE
    jedis.lrange("userId:" + userId.toString, 0, num)
      .asScala
      .map { item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  // 获取当前商品的相似列表，并过滤掉用户已经评分过的，作为备选列表
  def getTopSimProducts(num: Int, productId: Int, userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       : Array[Int] = {
    // 从广播变量相似度矩阵中拿到当前商品的相似度列表
    val allSimProducts = simProducts(productId).toArray
    val database = ConnHelper.mongoClient.getDatabase(this.mongoConfig.db)
    // 获得用户已经评分过的商品，过滤掉，排序输出
    val ratingCollection = database.getCollection(MONGODB_RATING_COLLECTION)

    val filter = Filters.eq("userId", userId)

    // 2) 查询并塞入 Scala Set[Int]
    // 注意：Document 中 productId 可能是 Int 或 Long，下面做了兼容处理
    val ratingExist: Set[Int] = {
      val it = ratingCollection.find(filter).iterator()
      it.asScala.map { doc =>
        // 尝试按 Int 取；若存的是 Long，则转为 Int
        val value = Option(doc.get("productId")).orNull
        if (value == null) {
          throw new IllegalStateException("document has no productId field: " + doc)
        } else {
          // 常见两种情况：Integer 或 Long
          value match {
            case i: java.lang.Integer => i.intValue()
            case l: java.lang.Long    => l.intValue()
            case other =>
              // 兜底：尝试 toString -> Int（不推荐长期使用，便于迁移）
              other.toString.toInt
          }
        }
      }.toSet
    }

    // 3) 过滤、排序、取前 num
    val result: Array[Int] = allSimProducts
      .filterNot { case (pid, _) => ratingExist.contains(pid) }
      .sortBy(-_._2)
      .take(num)
      .map(_._1)

    result
  }

  // 计算每个备选商品的推荐得分
  def computeProductScore(candidateProducts: Array[Int], userRecentlyRatings: Array[(Int, Double)],
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  : Array[(Int, Double)] = {
    // 定义一个长度可变数组ArrayBuffer，用于保存每一个备选商品的基础得分，(productId, score)
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义两个map，用于保存每个商品的高分和低分的计数器，productId -> count
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()
    // 遍历每个备选商品，计算和已评分商品的相似度
    for (candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings) {
      // 从相似度矩阵中获取当前备选商品和当前已评分商品间的相似度
      val simScore = getProductsSimScore(candidateProduct, userRecentlyRating._1, simProducts)
      if (simScore > 0.4) { // 按照公式进行加权计算，得到基础评分
        scores += ((candidateProduct, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) increMap(candidateProduct) = increMap
          .getOrElse(candidateProduct, 0) + 1
        else decreMap(candidateProduct) = decreMap.getOrElse(candidateProduct, 0) + 1
      }
    }

    // 根据公式计算所有的推荐优先级，首先以productId做groupby
    scores.groupBy(_._1).map {
      case (productId, scoreList) =>
        (productId, scoreList.map(_._2).sum / scoreList.length
          + log(increMap.getOrElse(productId, 1))
          - log(decreMap.getOrElse(productId, 1)))
    }
      // 返回推荐列表，按照得分排序
      .toArray.sortWith(_._2 > _._2)
  }

  def getProductsSimScore(product1: Int, product2: Int, simProducts: scala.collection.Map[Int,
    scala.collection.immutable.Map[Int, Double]]): Double = {
    simProducts.get(product1) match {
      case Some(sims) => sims.get(product2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // 自定义log函数，以N为底
  def log(m: Int): Double = {
    val N = 10
    math.log(m) / math.log(N)
  }

  // 写入mongodb
  def saveDataToMongoDB(userId: Int, streamRecs: Array[(Int, Double)]): Unit = {
    var database = ConnHelper.mongoClient.getDatabase(this.mongoConfig.db)
    val streamRecsCollection = database.getCollection(STREAM_RECS)
    // 按照userId查询并更新
    var query = Filters.eq("userId", userId)
    streamRecsCollection.deleteMany(query)

    import scala.jdk.CollectionConverters._

    val recDocs = streamRecs.map { case (productId, score) =>
      new Document()
        .append("productId", productId)
        .append("score", score)
    }

    val doc = new Document()
      .append("userId",userId)
      .append( "recs", recDocs)
    streamRecsCollection.insertOne(doc)
  }

  /**
   * 核心算法流程：计算实时推荐并保存到 MongoDB。
   * @param batchDF 当前微批次的评分数据 (Dataset[Rating])
   * @param batchId 当前微批次的 ID
   */
  def processRatingsBatch(batchDF: org.apache.spark.sql.Dataset[Rating], batchId: Long): Unit = {

    // 检查当前批次是否有数据，避免处理空批次
    if (!batchDF.isEmpty) {
      println(s"Processing Batch ID: $batchId, Count: ${batchDF.count()}")

      // 转换为 RDD 并使用 foreach (在 driver 端运行一次，然后任务分发给 executor)
      batchDF.rdd.foreachPartition { ratingsPartition =>
        // ratingsPartition 是 Iterator[Rating]，代表当前分区的所有评分

        ratingsPartition.foreach { rating =>
          // 1. 从 redis 里取出当前用户的最近评分
          val userRecentlyRatings = getUserRecentlyRatings(
            // 假设 ConnHelper 是正确的单例/连接获取机制
            MAX_USER_RATING_NUM, rating.userId, ConnHelper.jedis
          )

          // 2. 从相似度矩阵中获取当前商品最相似的商品列表
          val candidateProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, rating.productId,
            rating.userId, simProcutsMatrixBC.value, // 广播变量
          )

          // 3. 计算推荐优先级，得到实时推荐列表
          val streamRecs = computeProductScore(candidateProducts, userRecentlyRatings,
            simProcutsMatrixBC.value
          )

          // 4. 把推荐列表保存到 mongodb
          saveDataToMongoDB(rating.userId, streamRecs)
        }
      }
    }
  }
}
