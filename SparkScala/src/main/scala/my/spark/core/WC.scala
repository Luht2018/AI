package my.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WC {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
                          setAppName("Word_Count").
                          setMaster("local[2]").
                          set("spark.default.parallelism","100")

    val sc = new SparkContext( conf )
    val data = sc.textFile("C:\\Users\\HP.DESKTOP-13H97D1\\Desktop\\History.txt")
    val wordsRDD: RDD[(String, Int)] = data.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
    wordsRDD.saveAsTextFile("./data/words.txt")
    wordsRDD.count()

  }
}
