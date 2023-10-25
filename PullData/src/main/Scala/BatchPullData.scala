import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, date_sub, lit}

import java.time.LocalDate
import java.util.Properties

object BatchPullData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("pull Environment Data").enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
    val jdbcUrl = "jdbc:mysql://192.168.100.101/shtd_industry"
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    spark.sql("show databases")
    val tableNames = List("EnvironmentData", "ChangeRecord", "BaseMachine", "ProduceRecord", "MachineData")
    tableNames.foreach(x => {
      val df = spark.read.jdbc(jdbcUrl, x, properties)
      df
        .withColumn("partition_date",
//          df.select(date_sub(current_date(),1).as("yesterday")).col("yesterday")
          spark.sql("select date_sub(current_date(),1) as yesterday").col("yesterday")
//          lit(LocalDate.now().minusDays(1).toString)
        )
        .write.partitionBy("partition_date").mode("overwrite").saveAsTable(s"ods.${x.toLowerCase}")
      spark.sql(s"show partitions ods.${x.toLowerCase}").show
    })


  }

}
