import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions


import java.sql.DriverManager
import java.util.Properties
object IndexCalculation5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 3").config("spark.sql.storeAssignmentPolicy","LEGACY").enableHiveSupport().getOrCreate()
    val properties = new Properties()
    properties.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    val con = DriverManager.getConnection("jdbc:clickhouse://localhost:8123", properties)
    con.prepareStatement("drop table if exists shtd_industry.machine_produce_timetop2").execute()
    con.prepareStatement(
      """
        |create table shtd_industry.machine_produce_timetop2(
        |machine_id int,first_time int,
        |second_time int)engine=MergeTree() primary key machine_id
        |""".stripMargin).execute()
    spark.sql(
      """
        |select * from dws.machine_produce_per_avgtime
        |""".stripMargin).show()
    import spark.implicits._
    spark.sql(
      """
        |select
        |     produce_machine_id,
        |     array_sort(collect_list(producetime)) time_list
        |from dws.machine_produce_per_avgtime
        |group by produce_machine_id
        |""".stripMargin).map(x=>{
        (x.getAs[Int]("produce_machine_id"),x.getAs[Seq[Long]]("time_list").head.toInt,
          x.getAs[Seq[Long]]("time_list").tail.head.toInt)})
      .toDF("machine_id","first_time","second_time")
      .write.mode("append").jdbc("jdbc:clickhouse://localhost:8123","shtd_industry.machine_produce_timetop2",properties)

    con.close()
    spark.stop

  }
}
