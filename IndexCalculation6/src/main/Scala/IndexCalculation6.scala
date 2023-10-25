import org.apache.spark.sql.SparkSession

import java.sql.DriverManager
import java.util.Properties

object IndexCalculation6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 6").enableHiveSupport().getOrCreate()
    spark.read.table("dwd.fact_environment_data").show()
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    properties.setProperty("driver","com.mysql.jdbc.Driver")
//    properties.setProperty("characterEncoding","utf-8")
    val jdbcUrl = "jdbc:mysql://localhost:3306/shtd_industry"
    val con = DriverManager.getConnection(jdbcUrl, properties)
    //手动建表
    con.prepareStatement(
      """
        |drop table if exists machine_humidityAVG_compare
        |""".stripMargin).execute()
    con.prepareStatement("create table machine_humidityAVG_compare(EnvoId int,factory_avg varchar(32),machine_avg varchar(32),comparison varchar(32),env_date_year varchar(32),env_date_month varchar(32))").execute()

    spark.sql(
      """
        |select EnvoId,factory_avg,machine_avg,
        |       case
        |           when machine_avg>factory_avg then "高"
        |           when machine_avg<factory_avg then "低"
        |           else "相同"
        |       end comparison,env_date_year,env_date_month
        |from (select
        |          EnvoId,
        |          year(InPutTime) env_date_year,
        |          month(InPutTime) env_date_month,
        |          avg(Humidity) over(partition by year(InPutTime),month(InPutTime)) factory_avg,
        |          avg(Humidity) over(partition by EnvoId,year(InPutTime),month(InPutTime)) machine_avg
        |      from
        |          dwd.fact_environment_data)t1
        |""".stripMargin)
//      .saveAsTable("dwd.testChar")
      .write.mode("append")
      .jdbc("jdbc:mysql://localhost:3306/shtd_industry","machine_humidityAVG_compare",properties)


    spark.stop

  }

}
