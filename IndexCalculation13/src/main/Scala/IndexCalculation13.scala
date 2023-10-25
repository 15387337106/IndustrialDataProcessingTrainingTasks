import org.apache.spark.sql.SparkSession

import java.util.Properties

object IndexCalculation13 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 13").enableHiveSupport().getOrCreate()
    spark.table("dwd.fact_change_record").show()
    print("te2")
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    properties.setProperty("createTableColumnTypes","state_start_time varchar(32),window_sum int,window_avg int")
    spark.sql(
      """
        |select concat(start_date_ymd,"_",start_hour) state_start_time,
        |       window_sum,window_avg
        |from (select start_date_ymd,
        |             start_hour,
        |             run_time,
        |             sum(run_time) over(order by start_hour rows BETWEEN 2 PRECEDING AND CURRENT ROW) window_sum,
        |             avg(run_time) over(order by start_hour rows BETWEEN 2 PRECEDING AND CURRENT ROW) window_avg,
        |             row_number() over(order by start_hour) row_index
        |      from (select
        |                first (to_date(ChangeStartTime, "yyyy-MM-dd")) start_date_ymd,
        |                date_format (ChangeStartTime,"HH") start_hour,
        |                sum (unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) run_time
        |            from
        |                dwd.fact_change_record
        |            where ChangeStartTime between "2021-10-13 00:00:00" and "2021-10-14 00:00:00" and ChangeRecordState="运行"
        |            group by start_hour sort by start_hour) t1)t1
        |where row_index>2
        |""".stripMargin).write.mode("overwrite")
      .jdbc("jdbc:mysql://localhost:3306/shtd_industry","slide_window_runnning",properties)
print("--------------test2--------fw-------------")
  }
}
