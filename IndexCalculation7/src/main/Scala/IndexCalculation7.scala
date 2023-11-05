import org.apache.spark.sql.SparkSession

import java.sql.DriverManager
import java.util.Properties

object IndexCalculation7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 7").config("spark.sql.storeAssignmentPolicy","LEGACY")
      .enableHiveSupport().getOrCreate()
    spark.read.table("dwd.fact_change_record").show()
    spark.read.table("dwd.dim_machine").show()
    spark.sql("use dwd")
    val properties = new Properties()
    properties.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    val jdbcUrl = "jdbc:clickhouse://master:8123"
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val con = DriverManager.getConnection(jdbcUrl, properties)
    con.prepareStatement("drop table if exists shtd_store.machine_running_compare").execute()
    con.prepareStatement("create table shtd_store.machine_running_compare(start_month String,machine_factory int,comparison String,factory_avg String,company_avg String,primary key(start_month,machine_factory))engine=MergeTree()").execute()
    con.close()
//    println("---------------------------------------------------------------------------")
//    spark.sql(
//      """
//        |               SELECT
//        |                   MachineFactory,
//        |                   AVG(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) AS factory_avg,
//        |                   concat(year(ChangeStartTime), "-", month(ChangeStartTime)) AS start_month
//        |               FROM dwd.fact_change_record
//        |                        JOIN dwd.dim_machine ON BaseMachineID = ChangeMachineID
//        |               WHERE ChangeRecordState = "运行"
//        |               GROUP BY MachineFactory, start_month
//        |""".stripMargin).show()

//    spark.sql(
//      """
//        |SELECT
//        |          start_month,
//        |          MachineFactory,
//        |          factory_avg,
//        |          AVG(factory_avg) OVER () AS company_avg
//        |      FROM (
//        |               SELECT
//        |                   MachineFactory,
//        |                   AVG(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) AS factory_avg,
//        |                   concat(year(ChangeStartTime), "-", month(ChangeStartTime)) AS start_month
//        |               FROM dwd.fact_change_record
//        |                        JOIN dwd.dim_machine ON BaseMachineID = ChangeMachineID
//        |               WHERE ChangeRecordState = "运行"
//        |               GROUP BY MachineFactory, start_month
//        |           ) t1
//        |
//        |""".stripMargin).show()
    spark.sql(
      """
        |SELECT
        |    start_month,
        |    machine_factory,
        |    CASE
        |        WHEN factory_avg > company_avg THEN "高"
        |        WHEN factory_avg < company_avg THEN "低"
        |        ELSE "相同"
        |        END AS comparison,factory_avg,company_avg
        |from (SELECT
        |          start_month,
        |          MachineFactory machine_factory,
        |          factory_avg,
        |          AVG(factory_avg) OVER () AS company_avg
        |      FROM (
        |               SELECT
        |                   MachineFactory,
        |                   AVG(unix_timestamp(ChangeEndTime) - unix_timestamp(ChangeStartTime)) AS factory_avg,
        |                   concat(year(ChangeStartTime), "-", month(ChangeStartTime)) AS start_month
        |               FROM dwd.fact_change_record
        |                        JOIN dwd.dim_machine ON BaseMachineID = ChangeMachineID
        |               WHERE ChangeRecordState = "运行"  and ChangeEndTime is not null
        |               GROUP BY MachineFactory, start_month
        |           ) t1)t2
        |""".stripMargin)
      .write.mode("append")
      .jdbc(jdbcUrl,"shtd_store.machine_running_compare",properties)
  }

}
