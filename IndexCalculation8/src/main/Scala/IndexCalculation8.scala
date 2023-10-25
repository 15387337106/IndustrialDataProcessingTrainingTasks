import org.apache.spark.sql.SparkSession

object IndexCalculation8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 8").enableHiveSupport().getOrCreate()
    spark.read.table("dwd.fact_change_record").show()
//    spark.sql(
//      """
//        |
//        |select
//        |    ChangeMachineID,
//        |    ChangeStartTime,
//        |    ChangeRecordState,
//        |    count(*) over(partition by ChangeMachineID,ChangeRecordState) cnt
//        |from
//        |    dwd.fact_change_record
//        |""".stripMargin).show()
    import spark.implicits._
    spark.sql(
      """
        |select
        |    ChangeMachineID,
        |    array_sort(collect_list(concat_ws("#",row_num,ChangeRecordState,ChangeStartTime,ChangeEndTime))) row_status
        |from
        |(select
        |     ChangeMachineID,
        |     ChangeStartTime,
        |     ChangeEndTime,
        |     ChangeRecordState,
        |     row_number() over(partition by ChangeMachineID order by ChangeStartTime) row_num
        | from
        |     dwd.fact_change_record)t1
        | group by ChangeMachineID
        |""".stripMargin).map(x=>{
      val tuples = x.getAs[Seq[String]]("row_status")
      val data = tuples(tuples.length - 2).split("#")
        (x.getAs[Int]("ChangeMachineID"),data(1),data(2),data(3))
    }).toDF("machine_id","record_state","change_start_time","change_end_time").show(2000)
//    count(*) over(partition by ChangeMachineID order by ChangeRecordState) cnt
  }
}
