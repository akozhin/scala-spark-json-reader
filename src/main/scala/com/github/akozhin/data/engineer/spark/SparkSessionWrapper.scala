package com.github.akozhin.data.engineer.spark

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}