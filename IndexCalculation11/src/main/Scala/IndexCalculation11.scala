import org.apache.spark.sql.{Row, SparkSession}

import java.util.Properties

object IndexCalculation11 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 11").enableHiveSupport().getOrCreate()
    spark.read.table("dws.machine_data_total_time").show()
//  import spark.implicits._
//    spark.sql(
//      """
//        |select
//        | *,dense_rank() over(partition by machine_record_date order by total_time desc) rank
//        |from
//        |dws.machine_data_total_time
//        |""".stripMargin).show()
    spark.sql(
      """
        |select
        |   rank,machine_id,
        |   total_time,
        |   day
        |from (select
        |       machine_id,
        |       total_time,
        |       machine_record_date day,
        |       dense_rank() over(partition by machine_record_date order by total_time desc) rank
        |from
        |dws.machine_data_total_time)t1
        |where rank<=3 sort by day,rank
        |""".stripMargin).createTempView("t")
    spark.sql("select * from t").show()
    val properties = new Properties()
    properties.setProperty("driver","com.clickhouse.jdbc.ClickHouseDriver")
    properties.setProperty("createTableOptions","engine=MergeTree() primary key day")
    spark.sql(
      """
        |SELECT *
        |  FROM t
        | PIVOT (
        |    SUM(machine_id) id,sum(total_time) as time
        |    FOR rank IN (1 as first, 2 as second, 3 as tertiary)
        |)
        |""".stripMargin).select("day","first_id","second_id","tertiary_id","first_time","second_time","tertiary_time")
      .toDF().write
      .mode("append").jdbc("jdbc:clickhouse://localhost:8123/shtd_store","machine_data_total_time_top3",properties)
    //       写入数据类型String 自动变为string 报错
    //      .option("createTableColumnTypes","day String,first_id int,second_id int,tertiary_id int,first_time int,second_time int,tertiary_time int")
    println("gogogo")
  }

}
