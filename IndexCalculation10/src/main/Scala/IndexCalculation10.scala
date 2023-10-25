import org.apache.spark.sql.SparkSession

object IndexCalculation10 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 10").enableHiveSupport().getOrCreate()
    spark.read.table("dwd.fact_machine_data").show()

//    spark.sql(
//      """
//        |select to_date(MachineRecordDate,"yyyy-MM") d_date,MachineID,MachineRecordState,MachineRecordDate,
//        |       dense_rank() over(partition by to_date(MachineRecordDate,"yyyy-MM"),MachineID,MachineRecordState
//        |        order by MachineRecordDate) rank_d
//        |from dwd.fact_machine_data
//        |""".stripMargin).show(20000)
    spark.sql(
      """
        |select
        |    substring(MachineRecordDate,0,10) as machine_record_date,
        |    MachineRecordID,MachineID as machine_id,MachineRecordState,MachineRecordDate,
        |    lead(MachineRecordDate) over(partition by substring(MachineRecordDate,0,10),MachineID order by MachineRecordDate) dt,
        |    unix_timestamp(lead(MachineRecordDate) over(partition by substring(MachineRecordDate,0,10),MachineID order by MachineRecordDate))-unix_timestamp(MachineRecordDate) period
        |from
        |    dwd.fact_machine_data
        |""".stripMargin).createTempView("temp")
    spark.sql(
      """
        |select
        |machine_id,
        |machine_record_date,
        |cast(sum(period) as int) total_time
        |from temp
        |where MachineRecordState="运行" and dt is not null
        |group by machine_record_date,machine_id
        |""".stripMargin).union(
      spark.sql(
        """
          |select
          |machine_id,
          |machine_record_date,
          |0 total_time
          |from temp
          |where MachineRecordState="运行" and dt is null
          |group by machine_record_date,machine_id
          |""".stripMargin)
      ).write
      .mode("overwrite")
      .saveAsTable("dws.machine_data_total_time")
    print("----------------------")

















//    spark.sql(
//      """
//        |select to_date(MachineRecordDate,"yyyy-MM") d_date,MachineID,MachineRecordState,MachineRecordDate,
//        |       dense_rank() over(partition by to_date(MachineRecordDate,"yyyy-MM"),MachineID,MachineRecordState
//        |        order by to_date(MachineRecordDate,"yyyy-MM")) rank_d
//        |from dwd.fact_machine_data
//        |""".stripMargin).show(2000)
//    spark.sql(
//      """
//        |select MachineID,to_date(MachineRecordDate,"yyyy-MM") d_date,MachineRecordDate
//        |       MachineRecordState,row_number()
//        |       over(partition by
//        |       to_date(MachineRecordDate,"yyyy-MM")
//        |       ,MachineID order by MachineRecordDate) row
//        |from dwd.fact_machine_data
//
//        |""".stripMargin).show(2000)
//
//    spark.sql(
//      """
//        |select MachineID,d_date,MachineRecordState,array_sort(collect_list(concat_ws("-",row,d_date,MachineRecordState))) as flatData
//        |from (select MachineID,to_date(MachineRecordDate,"yyyy-MM") d_date,
//        |             MachineRecordState,row_number()
//        |             over(partition by MachineID,
//        |             MachineRecordState order by to_date(MachineRecordDate,"yyyy-MM")) row
//        |      from dwd.fact_machine_data
//        |      group by MachineID,d_date,MachineRecordState)t1
//
//        |""".stripMargin).show(1000)

//    val a = spark.table("dwd.fact_machine_data")
//      .selectExpr("substr(MachineRecordDate,0,10) as machine_record_date", "MachineRecordID", "MachineID as machine_id", "MachineRecordState", "MachineRecordDate")
//      .selectExpr("machine_record_date", "MachineRecordID", "machine_id", "MachineRecordState", "MachineRecordDate"
//        , "lead(MachineRecordDate) over(partition by machine_record_date,machine_id order by MachineRecordDate) etime"
//        , "UNIX_TIMESTAMP(lead(MachineRecordDate) over(partition by machine_record_date,machine_id order by MachineRecordDate),'yyyy-MM-dd HH:mm:ss')- UNIX_TIMESTAMP(MachineRecordDate,'yyyy-MM-dd HH:mm:ss') period"
//      )
//    a.show()
//    val b = a.filter("MachineRecordState = '运行' and etime is not null")
//      .groupBy("machine_record_date", "machine_id").agg(sum("period").as("total_time"))
//
//    val c = a.filter("MachineRecordState = '运行' and etime is  null")
//      .selectExpr("machine_record_date", "machine_id", "0 as total_time")
//
//    b.union(c)
  }

}
