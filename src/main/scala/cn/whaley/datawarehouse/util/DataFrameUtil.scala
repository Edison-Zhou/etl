package cn.whaley.datawarehouse.util

import java.util.{Calendar, Date}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}

/**
  * Created by Tony on 17/1/24.
  */
object DataFrameUtil {

  def dfZipWithIndex(df: DataFrame,
                     colName: String = "sk",
                     offset: Long = 0,
                     inFront: Boolean = true
                    ): DataFrame = {
    val result = df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset + 1) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset + 1))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName, LongType, false)))
      )
    )
    result
  }


  def dfAddIndex(df: DataFrame,
              colName: String,
              orderByColumn: Column,
              offset: Long = 0
             ): DataFrame = {
    val windowSpec = Window.orderBy(orderByColumn)
    df.withColumn(colName, row_number().over(windowSpec).plus(offset))
  }


  def addDimTime(df: DataFrame,
                 validTime: Date,
                 invalidTime: Date
                ): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.map(ln =>
        Row.fromSeq(ln.toSeq ++
          Seq(if (validTime == null) null else new java.sql.Timestamp(validTime.getTime),
            if (invalidTime == null) null else new java.sql.Timestamp(invalidTime.getTime)))
      )
      ,
      StructType(
        df.schema.fields
          ++ Array(StructField("dim_valid_time", TimestampType)
          , StructField("dim_invalid_time", TimestampType)))
    )
  }
}
