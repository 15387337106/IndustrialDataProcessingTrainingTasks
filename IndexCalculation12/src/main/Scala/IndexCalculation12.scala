import org.apache.spark.sql.SparkSession

import java.util.Properties

object IndexCalculation12 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 12").enableHiveSupport().getOrCreate()
    spark.table("dwd.fact_change_record").show()
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    properties.setProperty("createTableColumnTypes","start_date varchar(32),start_hour varchar(32),hour_add_standby int,day_agg_standby int")
    spark.sql(
      """
        |select
        |    start_date,
        |    start_hour,
        |    run_time-lag(run_time,1,0) over(order by run_time) hour_add_standby,
        |    sum(run_time) over(order by run_time) day_agg_standby
        |from (select
        |          first(to_date(ChangeStartTime,"yyyy-MM-dd")) start_date,hour(ChangeStartTime) start_hour,
        |          sum(unix_timestamp(ChangeEndTime)-unix_timestamp(ChangeStartTime)) run_time
        |      from
        |          dwd.fact_change_record
        |      where ChangeStartTime between "2021-10-12 00:00:00" and "2021-10-13 00:00:00" and ChangeRecordState="待机"
        |      group by start_hour)t1
        |""".stripMargin).write.mode("overwrite")
      .jdbc("jdbc:mysql://localhost:3306/shtd_industry","accumulate_standby",properties)
    print("go!go!!!!dqq")
  }

}
