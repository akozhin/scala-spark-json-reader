package com.github.akozhin.data.engineer.spark

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, Formats}
import org.apache.spark.sql.SparkSession

object JsonReader extends App {
  implicit val formats: Formats = DefaultFormats


  override def main(args: Array[String]): Unit = {
    case class Wine(id: Option[Long], country: Option[String], points: Option[Long], title: Option[String], variety: Option[String], winery: Option[String])

    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()
    val sc = spark.sparkContext
    val file = args(0)
    sc.textFile(file)
      .map(str => parse(str).extract[Wine])
      .foreach(w => println(w))

  }
}
