import org.apache.spark.sql.SparkSession

import java.sql.DriverManager
import java.util.Properties

object IndexCalculation9 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("index calculation 9").enableHiveSupport().getOrCreate()
    spark.read.table("dwd.fact_environment_data").show(10000)
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123456")
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    print("----手动建表")
    val jdbcUrl="jdbc:mysql://localhost:3306/shtd_industry"
    val con = DriverManager.getConnection(jdbcUrl,properties)
    con.prepareStatement("drop table if exists machine_runningAVG_compare").execute()
    con.prepareStatement(
      """
        |create table
        |machine_runningAVG_compare(
        |base_id int,machine_avg varchar(32),
        |factory_avg varchar(32),comparison varchar(32),
        |env_date_year varchar(32),env_date_month varchar(32));
        |""".stripMargin).execute()
    con.close()
    spark.sql(
      """
        |select BaseID,machine_avg,factory_avg,
        |       case when
        |                machine_avg>factory_avg then "高"
        |            when machine_avg<factory_avg then "低"
        |            end comparison,env_date_year,env_date_month
        |from
        |    (select
        |         BaseID,env_date,machine_avg,
        |         avg(machine_avg) over() factory_avg,year(env_date) env_date_year,
        |         month(env_date) env_date_month
        |     from (select
        |         BaseID,substring(InPutTime,0,7) as env_date,
        |         avg(PM10) as machine_avg
        |         from
        |         dwd.fact_environment_data
        |         group by BaseID,env_date)t1)t2
        |""".stripMargin).write.mode("overwrite").jdbc(jdbcUrl,"machine_runningAVG_compare",properties)
  }

}
