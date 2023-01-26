package com.sparkpsql

import scala.collection.mutable.Stack

import org.apache.spark.sql.functions.{col, monotonically_increasing_id, to_timestamp}
import org.apache.spark.sql.{SaveMode, SparkSession}

import com.sparkpsql.postgresSparkConnUtil.extractData
import com.sparkpsql.postgresSparkConnUtil.upsertData

import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

import com.typesafe.config.{Config, ConfigFactory}

class sparkPostgresTest extends AnyFunSuite {

    val spark: SparkSession = sparkSession.initSparkSession
    
    // Call config
    val applicationConf: Config = ConfigFactory.load("application.conf")

    // Set config utils
    val url = applicationConf.getString("postgres.url")
    val user = applicationConf.getString("postgres.user")
    val password = applicationConf.getString("postgres.password")
    val database = applicationConf.getString("postgres.database")
    val port = applicationConf.getString("postgres.port")
    val driver = applicationConf.getString("postgres.driver")
    val dbTable = applicationConf.getString("postgres.dbTable")

    /*test*/ignore("Spark + Postgres Data load test") {
        val df = spark.read.format("csv").option("header","true").load("src/main/resources/bread_types.csv")

        //val df2 = df.select(col("price_per_unit").cast("double").as("price_per_unit"))

        val requiredDF = df.withColumn("id", monotonically_increasing_id())
            .withColumn("price_per_unit",col("price_per_unit").cast("double"))
            .select("id","name","price_per_unit")

        requiredDF.show()

        val configs = Map(
            "url" -> url,
            "user" -> user,
            "password" -> password,
            "database" -> database,
            "port" -> port,
            "driver" -> driver
        )

        requiredDF.write.format("jdbc").options(configs).option(
            "dbTable", "bread_types"
        ).mode(SaveMode.Append).save()

        requiredDF.show()
        requiredDF.printSchema()
    }

    test("Spark + Postgres Data reading test") {
        
        val configs = Map(
            "url" -> url,
            "user" -> user,
            "password" -> password,
            "database" -> database,
            "port" -> port,
            "driver" -> driver,
            "dbTable" -> dbTable,
            "numPartitions" -> "2",
            "partitionColumn" -> "id",
            "lowerBound" -> "1",
            "upperBound" -> "24"
        )

        val extractedDF = extractData(spark,configs)
        extractedDF.explain()
        extractedDF.show(24) // default show method only displays 20 rows
        extractedDF.printSchema()
    }
}
