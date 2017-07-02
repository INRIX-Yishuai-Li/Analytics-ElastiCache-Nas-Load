package com.inrix.analytics.nas

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by Yishuai.Li on 11/22/2016.
  */
object NasDumpGenerator {

  def computeAndLoad(sc: SparkContext, s3Location: String, mapVersion: String, storeLocation: String): Unit = {
    println("s3Location: " + s3Location)

    val oneRd2 = sc.textFile(s3Location, 1000)
    //val counter = oneRd.count()

    //val oneRd2 = oneRd.repartition(900).persist(StorageLevel.MEMORY_AND_DISK)

    val processed = oneRd2.map(line => {
      val llist = line.split("\t")
      (llist(0), (llist(1), llist(2)))} )
      .groupByKey().map(x => beautify(x._1, fillIn(x._2), mapVersion))

/*    val processed = oneRd.map(line => {
      val llist = line.split("\t")
      (llist(0), List((llist(1), llist(2))))} )
      .reduceByKey( _ ::: _ ).map(x => beautify(x._1, fillIn(x._2), mapVersion))*/

    println("storeToLocation: " + storeLocation)
    processed.saveAsTextFile(storeLocation)
    println(s"Map $mapVersion finished")
  }

  def beautify(in1: String, in2: String, mapVersion: String): String =
  {
    "RPUSH\t" +
      "\"" + mapVersion + "-" + in1 + "\"\t" +
      in2 + "\n"
  }

  def fillIn(input: Iterable[(String, String)]): String =
  {

    val sorted = scala.util.Sorting.stableSort(
      input.toList,
      (_._1.toInt < _._1.toInt) : ((String, String),(String, String)) => Boolean )

    var index = 0
    val resultList: Array[String] = new Array[String](960)
    for (i <- 0 until 960) {
      if (index >= sorted.length || i != sorted(index)._1.toInt){
        resultList(i) = "\"-1\""
      } else {
        resultList(i) = "\"" + sorted(index)._2 + "\""
        index += 1
      }
    }

    resultList.mkString("\t")
  }
}
