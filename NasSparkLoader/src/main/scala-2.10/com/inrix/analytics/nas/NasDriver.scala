package com.inrix.analytics.nas

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Yishuai.Li on 11/21/2016.
  */
object NasDriver {

  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("generate nas dump")

    loadAllMap(conf)

  }

  def loadAllMap(sparkConf: SparkConf ): Unit = {
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAIFEYF3PYZ67CS5IA")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    val mapList = List("1701")

    mapList.foreach(
      mapVersion =>
        NasDumpGenerator.computeAndLoad(
          sc,
          s"s3n://inrixprod-referencedata/data/dbname=nas/mapversion=${mapVersion}/tablename=csegnas/data/*.bz2",
          mapVersion,
          s"s3n://inrix-analytics-scratch/loadNas/forRedis/mapversion=${mapVersion}/data")
    )
  }

}
