package com.rishabh.spark.datasource.s3


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by Rishabh on 12/04/16.
  */
case class sample(id: Integer)

class S3Relation(accesskey: String, secretkey: String, fileType: String, bucket: String, path:
String, write: Boolean)
                (@transient
                 val sqlContext: SQLContext) extends BaseRelation with TableScan {

  import sqlContext.implicits._

  val dummyData = Seq(sample(1))
  var df = sqlContext.sparkContext.parallelize(dummyData, 4).toDF()
  val s3Path = "s3a://" + bucket + path

  val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
  hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  hadoopConf.set("fs.s3a.access.key", accesskey)
  hadoopConf.set("fs.s3a.secret.key", secretkey)

  override def schema: StructType = {
    fileType match {
      case "json" =>
        df = sqlContext.read.json(s3Path)
      case "csv" =>
        df = sqlContext.read.format("com.databricks.spark.csv").load(s3Path)
      case "parquet" =>
        df = sqlContext.read.parquet(s3Path)
    }
    df.schema
  }

  override def buildScan(): RDD[Row] = {
    df.rdd
  }
}
