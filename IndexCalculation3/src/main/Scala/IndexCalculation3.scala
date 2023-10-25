import org.apache.spark.sql.SparkSession

object IndexCalculation3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 3").enableHiveSupport().getOrCreate()
    spark.sql(
      """
        |select
        |    ProduceRecordId produce_record_id,
        |    ProduceMachineId produce_machine_id,
        |    unix_timestamp(ProduceCodeEndTime)-unix_timestamp(ProduceCodeStartTime) producetime,
        |    avg(unix_timestamp(ProduceCodeEndTime)-unix_timestamp(ProduceCodeStartTime))
        |        over(partition by ProduceMachineID) produce_per_avgtime
        |from
        |    dwd.fact_produce_record
        |where ProduceCodeEndTime!='1900-01-01 00:00:00'
        |""".stripMargin).dropDuplicates(Array("produce_record_id","produce_machine_id"))
      .write.mode("overwrite").saveAsTable("dws.machine_produce_per_avgtime")

    spark.stop

  }
}
