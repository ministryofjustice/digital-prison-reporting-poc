package dpr.spike.service

import org.apache.spark.sql.types.DataType

class SourceReference(key: String, source: String, table: String, primaryKey: String,
                      partition_key: String, schema: DataType) {

  def getKey: String = key

  def getSource: String = source

  def getTable: String = table

  def getPrimaryKey: String = primaryKey

  def getPartitionKey: String = partition_key

  def getSchema: DataType = schema

//  def getCasts: Map[String, String] = null



}