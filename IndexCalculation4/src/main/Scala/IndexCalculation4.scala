import org.apache.spark.sql.SparkSession

import java.sql.DriverManager
import java.util.Properties

object IndexCalculation4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 3").enableHiveSupport().getOrCreate()
    val properties = new Properties()
    properties.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    val con = DriverManager.getConnection("jdbc:clickhouse://localhost:8123", properties)
    con.prepareStatement("drop table if exists machine_produce_per_avgtime").execute()
    con.prepareStatement(
      """
        |create table shtd_industry.machine_produce_per_avgtime(
        |produce_record_id int,produce_machine_id int,
        |producetime int,produce_per_avgtime int,
        |primary key (produce_record_id,produce_machine_id))engine=MergeTree()
        |""".stripMargin).execute()
    spark.sql(
        """
          |
          |select
          |produce_record_id,produce_machine_id,producetime,produce_per_avgtime
          |from (select
          |          ProduceRecordId produce_record_id,
          |          ProduceMachineId produce_machine_id,
          |          unix_timestamp(ProduceCodeEndTime)-unix_timestamp(ProduceCodeStartTime) producetime,
          |          avg(unix_timestamp(ProduceCodeEndTime)-unix_timestamp(ProduceCodeStartTime))
          |              over(partition by ProduceMachineID) produce_per_avgtime
          |      from
          |          dwd.fact_produce_record
          |      where ProduceCodeEndTime!='1900-01-01 00:00:00')t1
          |where producetime>produce_per_avgtime
          |""".stripMargin).dropDuplicates(Array("produce_record_id","produce_machine_id"))
      .write.mode("append").jdbc("jdbc:clickhouse://localhost:8123","shtd_industry.machine_produce_per_avgtime",properties)
    con.close()
    spark.stop

  }
}
