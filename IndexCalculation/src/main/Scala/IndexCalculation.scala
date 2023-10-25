import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, max, min, sum, unix_timestamp}

import java.sql.DriverManager
import java.util.Properties

object IndexCalculation {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    properties.setProperty("characterEncoding","utf-8")
    properties.setProperty("primaryKey","machine_id")
    properties.setProperty("createTableColumnTypes","machine_id int,change_record_state varchar(64),duration_time varchar(64),year int,month int")
    val jdbcUrl="jdbc:mysql://192.168.100.101:3306/shtd_industry"
    val spark = SparkSession.builder().appName("Index Calculation").enableHiveSupport().getOrCreate()
    var df = spark.read.table("dwd.fact_change_record").filter(col("ChangeEndTime").isNotNull)
    df.show()

    df.select(max("ChangeStartTime"),min("ChangeStartTime")).show
    df=df.groupBy(col("ChangeStartTime"),col("ChangeMachineID").as("machine_id"),date_format(col("ChangeRecordState"),"yyyy-MM")
      ,col("ChangeRecordState").as("change_record_state"))
      .agg(sum(unix_timestamp(col("ChangeEndTime"))-unix_timestamp(col("ChangeStartTime")))
        .as("duration_time"))
      .withColumn("year",date_format(col("ChangeStartTime"),"yyyy"))
      .withColumn("month",date_format(col("ChangeStartTime"),"MM"))
//    val con = DriverManager.getConnection(jdbcUrl, "root", "123456")
//    con.prepareStatement(
//      """
//        |create table machine_state_time(
//        |machine_id int,
//        |change_record_state varchar(64),
//        |duration_time varchar(64),
//        |year int,
//        |month int)
//        |""".stripMargin).execute()
//    con.close()


    df.select("machine_id","change_record_state","duration_time","year","month")
      .write.mode("append")
      .jdbc(jdbcUrl,"machine_state_time",properties)

//      """
//      SELECT
//        machine_id,
//        change_record_state,
//        duration_time,
//        year,
//        month
//      FROM
//        machine_state_time
//      ORDER BY
//        machine_id DESC,
//        duration_time DESC
//      LIMIT
//        10
//    """


    // 关闭 SparkSession
    spark.stop()

  }

}
