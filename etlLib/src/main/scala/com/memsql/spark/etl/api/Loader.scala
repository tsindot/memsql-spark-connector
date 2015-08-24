package com.memsql.spark.etl.api

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import com.memsql.spark.etl.api.configs.PhaseConfig

trait Loader extends Serializable {
  def load(dataframe: DataFrame, loadConfig: PhaseConfig) : Long
}
