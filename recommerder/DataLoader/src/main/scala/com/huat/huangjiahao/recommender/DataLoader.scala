package com.huat.huangjiahao.recommender

import com.mongodb.client.MongoClients
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document


/**
  * Product数据集
  * 3982                            商品ID
  * Fuhlen 富勒 M8眩光舞者时尚节能    商品名称
  * 1057,439,736                    商品分类ID，不需要
  * B009EJN4T2                      亚马逊ID，不需要
  * https://images-cn-4.ssl-image   商品的图片URL
  * 外设产品|鼠标|电脑/办公           商品分类
  * 富勒|鼠标|电子产品|好用|外观漂亮   商品UGC标签
  */
case class Product( productId: Int, name: String, imageUrl: String, categories: String, tags: String )

/**
  * Rating数据集
  * 4867        用户ID
  * 457976      商品ID
  * 5.0         评分
  * 1395676800  时间戳
  */
case class Rating( userId: Int, productId: Int, score: Double, timestamp: Int )

/**
  * MongoDB连接配置
  * @param uri    MongoDB的连接uri
  * @param db     要操作的db
  */
case class MongoConfig( uri: String, db: String )

object DataLoader {
  // 定义数据文件路径
  val PRODUCT_DATA_PATH = "/Users/mac/Desktop/e-recommend/recommerder/DataLoader/src/main/resources/products.csv"
  val RATING_DATA_PATH = "/Users/mac/Desktop/e-recommend/recommerder/DataLoader/src/main/resources/ratings.csv"
  // 定义mongodb中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://tiger.local:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 加载数据
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map( item => {
      // product数据通过^分隔，切分出来
      val attr = item.split("\\^")
      // 转换成Product
      Product( attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim )
    } ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map( item => {
      val attr = item.split(",")
      Rating( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    } ).toDF()

    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )
    storeDataInMongoDB( productDF, ratingDF )

    spark.stop()
  }
  def storeDataInMongoDB( productDF: DataFrame, ratingDF: DataFrame )(implicit mongoConfig: MongoConfig): Unit ={
    // 新建一个mongodb的连接，客户端
    val mongoClient = MongoClients.create(mongoConfig.uri)
    var database = mongoClient.getDatabase(mongoConfig.db)
    // 定义要操作的mongodb表，可以理解为 db.Product
    val productCollection = database.getCollection( MONGODB_PRODUCT_COLLECTION )
    val ratingCollection = database.getCollection( MONGODB_RATING_COLLECTION )

    // 如果表已经存在，则删掉
    productCollection.drop()
    ratingCollection.drop()

    // 将当前数据存入对应的表中
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("database", mongoConfig.db)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("mongodb")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("database", mongoConfig.db)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("mongodb")
      .save()

    // 对表创建索引
    productCollection.createIndex( new Document( "productId", 1 ) )
    ratingCollection.createIndex( new Document( "productId", 1 ) )
    ratingCollection.createIndex( new Document( "userId", 1 ) )

    mongoClient.close()
  }
}
