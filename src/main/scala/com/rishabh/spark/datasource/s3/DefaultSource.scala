package com.rishabh.spark.datasource.s3

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
/**
  * Created by Rishabh on 12/04/16.
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]):
  BaseRelation = {
    val accessKey = parameters.getOrElse("accesskey", sys.error("accesskey is required"))
    val secretKey = parameters.getOrElse("secretkey", sys.error("secretkey is required"))
    val fileType = parameters.getOrElse("type", sys.error("filetype is required"))
    val path = parameters.getOrElse("path", sys.error("path is required"))
    val bucket = parameters.getOrElse("bucketName", sys.error("bucket is required"))
    new S3Relation(accessKey, secretKey, fileType, bucket, path, false)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String,
    String], data: DataFrame): BaseRelation = {
    val accesskey = parameters.getOrElse("accesskey",sys.error("accesskey is required"))
    val secretkey = parameters.getOrElse("secretkey", sys.error("secretkey is required"))
    val bucket = parameters.getOrElse("bucketName", sys.error("bucket is required"))
    val fileType = parameters.getOrElse("type", sys.error("filetype is required"))
    val path = parameters.getOrElse("path", sys.error("path is required"))

    val supported = List("json", "parquet", "csv")
    if (!supported.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported.")
    }
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.access.key", accesskey)
    hadoopConf.set("fs.s3a.secret.key", secretkey)
    val s3Path = "s3a://" + bucket + path
    doSave(fileType, data, s3Path)

    new S3Relation(accesskey, secretkey, fileType, bucket, path, true)(sqlContext)
  }

  private def doSave(fileType: String, dataFrame: DataFrame, path: String) = {
    fileType match {
      case "json" =>
        dataFrame.write.json(path)
      case "parquet" =>
        dataFrame.write.parquet(path)
      case "csv" =>
        dataFrame.write.format("com.databricks.spark.csv").save(path)
    }
  }
}
