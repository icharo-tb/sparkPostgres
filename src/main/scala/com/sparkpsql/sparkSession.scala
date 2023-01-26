package com.sparkpsql

import org.apache.spark.sql.SparkSession

object sparkSession {
  
    def initSparkSession: SparkSession = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[4]").appName("sparkPostgres")
            .getOrCreate()
        
        spark
    }

}