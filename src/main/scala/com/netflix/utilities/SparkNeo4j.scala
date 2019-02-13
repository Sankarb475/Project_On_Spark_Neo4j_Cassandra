package com.netflix.utilities


import org.neo4j.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkNeo4j {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("APP_NAME")
      .setMaster("local")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.auth.username", "")
      .set("spark.cassandra.auth.password", "")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val neo = Neo4j(sc)

    //val rdd = neo.cypher("MATCH (n:Person) RETURN id(n) as id ").loadRowRdd

    val rdd = neo.cypher("match(L:Person) return L.id").loadRowRdd

    rdd.collect().foreach(println)

    print("other stuff")
    print(rdd.count)
    print("after it")

    print(rdd.first.schema.fieldNames)

  }

}
