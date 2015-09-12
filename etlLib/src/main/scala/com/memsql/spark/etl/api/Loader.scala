package com.memsql.spark.etl.api

import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.sql.DataFrame
import com.memsql.spark.etl.api.configs.PhaseConfig

trait Loader extends Serializable {
  def load(dataframe: DataFrame, loadConfig: PhaseConfig, logger: PhaseLogger) : Long
}
