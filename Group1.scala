import org.apache.hadoop.hdfs.server.datanode.ShortCircuitRegistry.NewShmInfo
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}




object Group1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new  SparkConf()
  sparkConf.set("spark.app.name", "Group1")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

  val orderDf = spark.read.option("header", true)
    .option("inferSchema", true)
    .csv("Group_1/grptable.csv")

 //orderDf.show()

  orderDf.createOrReplaceTempView("tableData")

  val res = spark.sql("select * from tableData where age = 25")

  res.show()



}
