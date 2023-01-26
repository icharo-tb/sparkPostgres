package com.sparkpsql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.mutable.{Map => MutableMap}

import com.typesafe.config.{Config, ConfigFactory}

object postgresSparkConnUtil {

    // Spark Configs
    def extractData(spark: SparkSession, configs: Map[String, String], readType: String = "fullRead"): DataFrame = {

        // "configs" argument will read string -> string from a given map

        // We will set jdbc driver options as our config map
        val options = setJdbcOptions(configs)
        val resultDf: DataFrame = readType match {
            case "fullRead" => fullRead(spark, options)
            case "incrementalRead" => incrementalRead(spark, options)
            case _ => throw new Exception(s"Only fullRead and incrementalRead are implemented.")
        }
        resultDf
    }

    /***
     * We will need to look for some important values:
        - query
        - url
        - user
        - password
        - driver
        - numPartitions
        - partitionColumn
        - lowerBound:   lower bound of the key column
        - upperBound:   upper bound of the key column
        - fetchSize:    optional jdbc import to look for fetching size
    */
    def setJdbcOptions(configs: Map[String, String]): MutableMap[String, String] = {
        val jdbcOptions = collection.mutable.Map(
            "url" -> configs.getOrElse("url", throw new Exception(s"Pass a correct 'url' key.")),
            "user" -> configs.getOrElse("user", throw new Exception(s"Pass a correct 'user' key.")),
            "password" -> configs.getOrElse("password", throw new Exception(s"Pass a correct 'password' key."))
        )

        // Check 'query' and 'dbTable' values
        if(configs.contains("query") && configs.contains("dbTable")){
            throw new IllegalArgumentException(s"Set only 'query' or 'dbTable', not both.")
        } else if(configs.contains("query")){
            jdbcOptions += ("query" -> configs("query"))
        } else if(configs.contains("dbTable")){
            jdbcOptions += ("dbTable" -> configs("dbTable"))
        } else{
            throw new Exception(s"No 'query' nor 'dbTable' value specified.")
        }

        // Check 'driver' value
        if(configs.contains("driver")){
            jdbcOptions += ("driver" -> configs("driver"))
        }

        // Check number of partitions value
        if(configs.contains("numPartitions")){
            if(configs.contains("partitionColumn") && configs.contains("lowerBound") && configs.contains("upperBound")){
                println("Check successful.")
                jdbcOptions += (
                    "numPartitions" -> configs("numPartitions"),
                    "partitionColumn" -> configs("partitionColumn"),
                    "lowerBound" -> configs("lowerBound"),
                    "upperBound" -> configs("upperBound")
                )
            } else{
                val errMessage = s"""
                Error: some data have been not found, user must specify ->

                +----------------------+
                | - numPartitions      |
                | - partitionColumn    |
                | - lowerBound         |
                | - upperBound         |
                +----------------------+
                """

                throw new IllegalArgumentException(errMessage)
            }
        }
        jdbcOptions
    }

    def fullRead(spark: SparkSession, configs: MutableMap[String, String]): DataFrame = {
        val resultDf = jdbcFetch(spark, configs)
        resultDf
    }

    def incrementalRead(spark: SparkSession, configs: MutableMap[String, String]): DataFrame = {
        spark.emptyDataFrame
    }

    def jdbcFetch(spark: SparkSession, options: MutableMap[String, String]): DataFrame = {
        
        try {
            val resultDf: DataFrame = spark.read.format(
                "jdbc"
            ).options(options).load()

            resultDf
        } catch {
            case ex: Exception => throw new UnsupportedOperationException(s"Error on jdbc Fetch: ${ex}")
        }
    }

    def upsertData(args: Array[String]): Unit = {
        println("")
    }
}
